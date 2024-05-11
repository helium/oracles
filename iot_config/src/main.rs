use anyhow::{Error, Result};
use clap::Parser;
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_proto::services::iot_config::{AdminServer, GatewayServer, OrgServer, RouteServer};
use iot_config::{
    admin::AuthCache, admin_service::AdminService, db_cleaner::DbCleaner,
    gateway_service::GatewayService, org, org_service::OrgService, region_map::RegionMapReader,
    route_service::RouteService, settings::Settings, telemetry,
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use task_manager::{ManagedTask, TaskManager};
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

        // Create database pool
        let pool = settings.database.connect("iot-config-store").await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let metadata_pool = settings.metadata.connect("iot-config-metadata").await?;

        let (auth_updater, auth_cache) = AuthCache::new(settings.admin_pubkey()?, &pool).await?;
        let (region_updater, region_map) = RegionMapReader::new(&pool).await?;
        let (delegate_key_updater, delegate_key_cache) = org::delegate_keys_cache(&pool).await?;

        let signing_keypair = Arc::new(settings.signing_keypair()?);

        let gateway_svc = GatewayService::new(
            settings,
            metadata_pool,
            region_map.clone(),
            auth_cache.clone(),
            delegate_key_cache,
        )?;

        let route_svc =
            RouteService::new(signing_keypair.clone(), auth_cache.clone(), pool.clone());

        let org_svc = OrgService::new(
            signing_keypair.clone(),
            auth_cache.clone(),
            pool.clone(),
            route_svc.clone_update_channel(),
            delegate_key_updater,
        )?;

        let admin_svc = AdminService::new(
            settings,
            auth_cache.clone(),
            auth_updater,
            pool.clone(),
            region_map.clone(),
            region_updater,
        )?;

        let listen_addr = settings.listen;
        let pubkey = settings
            .signing_keypair()
            .map(|keypair| keypair.public_key().to_string())?;
        tracing::debug!("listening on {listen_addr}");
        tracing::debug!("signing as {pubkey}");

        let grpc_server = GrpcServer {
            listen_addr,
            gateway_svc,
            route_svc,
            org_svc,
            admin_svc,
        };

        let db_cleaner = DbCleaner::new(pool.clone(), settings.deleted_entry_retention());

        TaskManager::builder()
            .add_task(grpc_server)
            .add_task(db_cleaner)
            .build()
            .start()
            .await
    }
}

pub struct GrpcServer {
    listen_addr: SocketAddr,
    gateway_svc: GatewayService,
    route_svc: RouteService,
    org_svc: OrgService,
    admin_svc: AdminService,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            let grpc_server = transport::Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(250)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .layer(tower_http::trace::TraceLayer::new_for_grpc())
                .add_service(GatewayServer::new(self.gateway_svc))
                .add_service(OrgServer::new(self.org_svc))
                .add_service(RouteServer::new(self.route_svc))
                .add_service(AdminServer::new(self.admin_svc))
                .serve(self.listen_addr)
                .map_err(Error::from);

            tokio::select! {
                _ = shutdown => {
                    tracing::warn!("grpc server shutting down");
                    Ok(())
                }
                res = grpc_server => {
                    match res {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            tracing::error!(?err, "grpc server failed with error");
                            Err(anyhow::anyhow!("grpc server exiting with error"))
                        }
                    }
                }
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
