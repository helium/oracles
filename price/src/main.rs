use anyhow::Result;
use chrono::Duration;
use clap::Parser;
use file_store::{file_sink, file_upload, FileType};
use helium_proto::BlockchainTokenTypeV1;
use price::{cli::check, PriceGenerator, Settings};
use std::path::{self, PathBuf};
use task_manager::TaskManager;

const PRICE_SINK_ROLL_MINS: i64 = 3;

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

        // Initialize uploader
        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;

        let store_base_path = path::Path::new(&settings.cache);

        let (price_sink, price_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::PriceReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_report_submission"),
        )
        .roll_time(Duration::minutes(PRICE_SINK_ROLL_MINS))
        .create()
        .await?;

        // price generators
        let hnt_price_generator =
            PriceGenerator::new(settings, BlockchainTokenTypeV1::Hnt, price_sink.clone()).await?;
        let mobile_price_generator =
            PriceGenerator::new(settings, BlockchainTokenTypeV1::Mobile, price_sink.clone())
                .await?;
        let iot_price_generator =
            PriceGenerator::new(settings, BlockchainTokenTypeV1::Iot, price_sink.clone()).await?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(price_sink_server)
            .add_task(hnt_price_generator)
            .add_task(mobile_price_generator)
            .add_task(iot_price_generator)
            .build()
            .start()
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
