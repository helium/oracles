use anyhow::{Error, Result};
use clap::Parser;
use file_store::{file_upload, traits::FileSinkWriteExt};
use futures_util::TryFutureExt;
use helium_proto::EntropyReportV1;
use poc_entropy::{entropy_generator::EntropyGenerator, server::ApiServer, Settings};
use std::{net::SocketAddr, path, time::Duration};
use tokio::{self, signal};

const ENTROPY_SINK_ROLL_SECS: u64 = 2 * 60;

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
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
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
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        // Initialize uploader
        let store_base_path = path::Path::new(&settings.cache);

        // entropy
        let mut entropy_generator = EntropyGenerator::new(&settings.source).await?;
        let entropy_watch = entropy_generator.receiver();

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;
        let (entropy_sink, entropy_sink_server) = EntropyReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            Some(Duration::from_secs(ENTROPY_SINK_ROLL_SECS)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        // server
        let socket_addr: SocketAddr = settings.listen.parse()?;
        let api_server = ApiServer::new(socket_addr, entropy_watch).await?;

        tracing::info!("api listening on {}", api_server.socket_addr);

        tokio::try_join!(
            api_server.run(&shutdown),
            entropy_generator
                .run(entropy_sink, &shutdown)
                .map_err(Error::from),
            entropy_sink_server
                .run(shutdown.clone())
                .map_err(Error::from),
            file_upload_server
                .run(shutdown.clone())
                .map_err(Error::from),
        )
        .map(|_| ())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
