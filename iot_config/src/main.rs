use anyhow::{Error, Result};
use clap::Parser;
use futures_util::TryFutureExt;
use helium_proto::services::iot_config::{
    GatewayServer, OrgServer, RouteServer, SessionKeyFilterServer,
};
use iot_config::{
    gateway_service::GatewayService, org_service::OrgService, route_service::RouteService,
    session_key_service::SessionKeyFilterService, settings::Settings,
};
use std::path::PathBuf;
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
pub struct Daemon {}

impl Daemon {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect(10).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let listen_addr = settings.listen_addr()?;

        let gateway_svc = GatewayService {};
        let org_svc = OrgService {};
        let route_svc = RouteService {};
        let session_key_filter_svc = SessionKeyFilterService {};

        transport::Server::builder()
            .add_service(GatewayServer::new(gateway_svc))
            .add_service(OrgServer::new(org_svc))
            .add_service(RouteServer::new(route_svc))
            .add_service(SessionKeyFilterServer::new(session_key_filter_svc))
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
