use anyhow::{Error, Result};
use clap::Parser;
use futures_util::TryFutureExt;
use helium_proto::services::iot_config::{AdminServer, GatewayServer, OrgServer, RouteServer};
use iot_config::{
    admin::AuthCache, admin_service::AdminService, gateway_service::GatewayService, org,
    org_service::OrgService, region_map::RegionMapReader, route_service::RouteService,
    settings::Settings, telemetry,
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
        telemetry::initialize();

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        // Create database pool
        let pool = settings.database.connect("iot-config-store").await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let metadata_pool = settings.metadata.connect("iot-config-metadata").await?;

        let listen_addr = settings.listen_addr()?;

        let (auth_updater, auth_cache) = AuthCache::new(settings, &pool).await?;
        let (region_updater, region_map) = RegionMapReader::new(&pool).await?;
        let (delegate_key_updater, delegate_key_cache) = org::delegate_keys_cache(&pool).await?;

        let gateway_svc = GatewayService::new(
            settings,
            metadata_pool,
            region_map.clone(),
            auth_cache.clone(),
            delegate_key_cache,
            shutdown_listener.clone(),
        )?;
        let route_svc = RouteService::new(
            settings,
            auth_cache.clone(),
            pool.clone(),
            shutdown_listener.clone(),
        )?;
        let org_svc = OrgService::new(
            settings,
            auth_cache.clone(),
            pool.clone(),
            route_svc.clone_update_channel(),
            delegate_key_updater,
            shutdown_listener.clone(),
        )?;
        let admin_svc = AdminService::new(
            settings,
            auth_cache.clone(),
            auth_updater,
            pool.clone(),
            region_map.clone(),
            region_updater,
        )?;

        let pubkey = settings
            .signing_keypair()
            .map(|keypair| keypair.public_key().to_string())?;
        tracing::debug!("listening on {listen_addr}");
        tracing::debug!("signing as {pubkey}");

        let server = transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(GatewayServer::new(gateway_svc))
            .add_service(OrgServer::new(org_svc))
            .add_service(RouteServer::new(route_svc))
            .add_service(AdminServer::new(admin_svc))
            .serve_with_shutdown(listen_addr, shutdown_listener)
            .map_err(Error::from);

        tokio::try_join!(server)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
