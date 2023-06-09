use anyhow::Result;
use chrono::Duration;
use clap::Parser;
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use poc_entropy::{entropy_generator::EntropyGenerator, server::ApiServer, Settings};
use std::{net::SocketAddr, path};
use task_manager::TaskManager;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const ENTROPY_SINK_ROLL_MINS: i64 = 2;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Entropy Server")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<path::PathBuf>,

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
    Server(Server),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Initialize uploader
        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings(&settings.output).await?;

        let store_base_path = path::Path::new(&settings.cache);

        // entropy
        let (entropy_sink, entropy_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::EntropyReport,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_report_submission"),
        )
        .file_upload(Some(file_upload))
        .roll_time(Duration::minutes(ENTROPY_SINK_ROLL_MINS))
        .create()
        .await?;

        let entropy_generator = EntropyGenerator::new(&settings.source, entropy_sink).await?;
        let entropy_watch = entropy_generator.receiver();

        // server
        let socket_addr: SocketAddr = settings.listen.parse()?;
        let api_server = ApiServer::new(socket_addr, entropy_watch).await?;

        tracing::info!("api listening on {}", api_server.socket_addr);

        TaskManager::builder()
            .add(file_upload_server)
            .add(entropy_sink_server)
            .add(entropy_generator)
            .add(api_server)
            .start()
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
