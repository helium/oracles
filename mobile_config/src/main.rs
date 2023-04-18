use anyhow::{Error, Result};
use clap::Parser;
use futures_util::TryFutureExt;
use helium_proto::services::mobile_config::{AdminServer, GatewayServer, RouterServer};
use jemallocator::Jemalloc;
use mobile_config::{
    admin_service::AdminService, gateway_service::GatewayService, key_cache::KeyCache,
    router_service::RouterService, settings::Settings,
};
use std::{path::PathBuf, time::Duration};
use tokio::signal;
use tonic::transport;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// We use jemalloc due to high allocation churn and fragmentation when
// using the system allocator.
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Config Service")]
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

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let (pool, pool_handle) = settings
            .database
            .connect("mobile-config-store", shutdown_listener.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let (metadata_pool, md_pool_handle) = settings
            .metadata
            .connect("mobile-config-metadata", shutdown_listener.clone())
            .await?;

        let listen_addr = settings.listen_addr()?;

        let (key_cache_updater, key_cache) = KeyCache::new(settings, &pool).await?;

        let admin_svc =
            AdminService::new(settings, key_cache.clone(), key_cache_updater, pool.clone())?;
        let gateway_svc = GatewayService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );
        let router_svc = RouterService::new(key_cache.clone(), settings.signing_keypair()?);

        let server = transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(AdminServer::new(admin_svc))
            .add_service(GatewayServer::new(gateway_svc))
            .add_service(RouterServer::new(router_svc))
            .serve_with_shutdown(listen_addr, shutdown_listener)
            .map_err(Error::from);

        tokio::try_join!(
            pool_handle.map_err(Error::from),
            md_pool_handle.map_err(Error::from),
            server,
        )?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
