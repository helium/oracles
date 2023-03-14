use anyhow::{Error, Result};
use clap::Parser;
use futures_util::TryFutureExt;
use helium_proto::services::iot_config::{
    AdminServer, GatewayServer, OrgServer, RouteServer, SessionKeyFilterServer,
};
use iot_config::{
    admin::AuthCache, gateway_service::GatewayService, org_service::OrgService,
    region_map::RegionMap, route_service::RouteService,
    session_key_service::SessionKeyFilterService, settings::Settings, AdminService,
};
use std::{path::PathBuf, time::Duration};
use tokio::signal;
use tonic::transport;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium IoT Config Service")]
pub struct Cli {
    /// Optional configuration file to use. If present, the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Daemon),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Daemon;

impl Daemon {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Create database pool
        let (pool, db_join_handle) = settings.database.connect(shutdown_listener.clone()).await?;
        sqlx::migrate!().run(&pool).await?;

        let listen_addr = settings.listen_addr()?;

        let auth_cache = AuthCache::new(settings, &pool).await?;
        let region_map = RegionMap::new(&pool).await?;

        let gateway_svc = GatewayService::new(settings, region_map.clone())?;
        let route_svc =
            RouteService::new(auth_cache.clone(), pool.clone(), shutdown_listener.clone());
        let org_svc = OrgService::new(
            auth_cache.clone(),
            pool.clone(),
            settings.network,
            route_svc.clone_update_channel(),
        );
        let admin_svc = AdminService::new(
            auth_cache.clone(),
            pool.clone(),
            region_map.clone(),
            settings.network,
        );
        let session_key_filter_svc = SessionKeyFilterService::new(
            auth_cache.clone(),
            pool.clone(),
            shutdown_listener.clone(),
        );

        let server = transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(GatewayServer::new(gateway_svc))
            .add_service(OrgServer::new(org_svc))
            .add_service(RouteServer::new(route_svc))
            .add_service(AdminServer::new(admin_svc))
            .add_service(SessionKeyFilterServer::new(session_key_filter_svc))
            .serve_with_shutdown(listen_addr, shutdown_listener)
            .map_err(Error::from);

        tokio::try_join!(db_join_handle.map_err(Error::from), server)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
