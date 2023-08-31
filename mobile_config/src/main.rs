use anyhow::{Error, Result};
use clap::Parser;
use futures_util::TryFutureExt;
use helium_proto::services::mobile_config::{
    AdminServer, AuthorizationServer, EntityServer, GatewayServer,
};
use mobile_config::{
    admin_service::AdminService, authorization_service::AuthorizationService,
    entity_service::EntityService, gateway_service::GatewayService, key_cache::KeyCache,
    settings::Settings,
};
use std::{path::PathBuf, time::Duration};
use tokio::signal;
use tonic::transport;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect("mobile-config-store").await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let metadata_pool = settings.metadata.connect("mobile-config-metadata").await?;

        let listen_addr = settings.listen_addr()?;

        let (key_cache_updater, key_cache) = KeyCache::new(settings, &pool).await?;

        let admin_svc =
            AdminService::new(settings, key_cache.clone(), key_cache_updater, pool.clone())?;
        let gateway_svc = GatewayService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );
        let auth_svc = AuthorizationService::new(key_cache.clone(), settings.signing_keypair()?);
        let entity_svc = EntityService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );

        transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .layer(tower_http::trace::TraceLayer::new_for_grpc())
            .add_service(AdminServer::new(admin_svc))
            .add_service(GatewayServer::new(gateway_svc))
            .add_service(AuthorizationServer::new(auth_svc))
            .add_service(EntityServer::new(entity_svc))
            .serve_with_shutdown(listen_addr, shutdown_listener)
            .map_err(Error::from)
            .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
