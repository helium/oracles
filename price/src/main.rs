use anyhow::Result;
use clap::Parser;
use file_store::{
    file_upload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
};
use helium_proto::PriceReportV1;
use price::{cli::check, PriceGenerator, Settings};
use std::{
    path::{self, PathBuf},
    time::Duration,
};
use task_manager::TaskManager;

const PRICE_SINK_ROLL_SECS: u64 = 3 * 60;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Price Oracle Server")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Env variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<path::PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        self.cmd.run(self.config).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Server),
    Check(Check),
}

impl Cmd {
    pub async fn run(&self, config: Option<PathBuf>) -> Result<()> {
        match self {
            Self::Server(cmd) => {
                let settings = Settings::new(config)?;
                custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
                tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
                cmd.run(&settings).await
            }
            Self::Check(options) => check::run(options.into()).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Check {
    #[clap(short, long)]
    iot: bool,
    #[clap(short, long)]
    mobile: bool,
}

impl From<&Check> for check::Mode {
    fn from(value: &Check) -> Self {
        if value.mobile {
            check::Mode::Mobile
        } else {
            check::Mode::Iot
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        let file_store_client = settings.file_store.connect().await;

        // Initialize uploader
        let (file_upload, file_upload_server) =
            file_upload::FileUpload::new(file_store_client.clone(), settings.output_bucket.clone())
                .await;

        let (price_sink, price_sink_server) = PriceReportV1::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(Duration::from_secs(PRICE_SINK_ROLL_SECS)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let mut task_manager = TaskManager::new();
        task_manager.add(file_upload_server);
        task_manager.add(price_sink_server);

        for token_setting in settings.tokens.iter() {
            task_manager.add(
                PriceGenerator::new(
                    settings,
                    token_setting.token,
                    token_setting.default_price,
                    price_sink.clone(),
                )
                .await?,
            );
        }

        task_manager.start().await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
