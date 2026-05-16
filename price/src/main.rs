use anyhow::Result;
use clap::Parser;
use file_store::file_upload;
use file_store_oracles::traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt};
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};
use helium_proto::PriceReportV1;
use price::{
    backfill,
    cli::check,
    iceberg,
    sinks::{IcebergPriceSink, PriceSink, S3PriceSink},
    PriceGenerator, Settings,
};
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
    Backfill(backfill::Cmd),
}

impl Cmd {
    pub async fn run(self, config: Option<PathBuf>) -> Result<()> {
        match self {
            Self::Server(cmd) => {
                let settings = Settings::new(config)?;
                custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
                tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
                cmd.run(&settings).await
            }
            Self::Check(options) => {
                let url = match options.url {
                    Some(url) => url,
                    None => Settings::new(config)?.source,
                };
                check::run(url).await
            }
            Self::Backfill(cmd) => {
                let settings = Settings::new(config)?;
                custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
                tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
                cmd.run(&settings).await
            }
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Check {
    /// Optional Hermes URL override. Defaults to the `source` setting.
    #[clap(short, long)]
    url: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        let mut task_manager = TaskManager::new();
        let mut sinks: Vec<Box<dyn PriceSink>> = Vec::new();

        // S3 sink: only enabled when `output` is configured. Both
        // file_upload and price_sink (the FileSinkClient<PriceReportV1>)
        // are spawned together; their handles drive the upload pipeline
        // and the PriceReportV1 file roll. The `S3PriceSink` wrapper
        // gives us a uniform `PriceSink` interface.
        if let Some(output) = settings.output.as_ref() {
            tracing::info!("output bucket configured, starting file_sink");
            let (file_upload, file_upload_server) =
                file_upload::FileUpload::from_bucket_client(output.connect().await).await;
            let (price_sink, price_sink_server) = PriceReportV1::file_sink(
                &settings.cache,
                file_upload.clone(),
                FileSinkCommitStrategy::Automatic,
                FileSinkRollTime::Duration(Duration::from_secs(PRICE_SINK_ROLL_SECS)),
                env!("CARGO_PKG_NAME"),
            )
            .await?;
            task_manager.add(file_upload_server);
            task_manager.add(price_sink_server);
            sinks.push(Box::new(S3PriceSink::new(price_sink)));
        }

        // Iceberg sink: optional. The `BatchedWriter` owns batching,
        // on-disk spool durability, and the snapshot commit. The task
        // is registered after the S3 servers so shutdown LIFO drains
        // pending records into Iceberg before tearing down upstream
        // pieces.
        if let Some(iceberg_settings) = settings.iceberg_settings.as_ref() {
            tracing::info!("iceberg settings provided, connecting...");
            let table = iceberg::connect_table(iceberg_settings).await?;
            let config = BatchedWriterConfig::new(settings.cache.join("iceberg-spool"))
                .with_max_batch_size(settings.iceberg_batch_size)
                .with_batch_timeout(settings.iceberg_batch_timeout);
            let (writer, batched_task) = BatchedWriter::new(table, config);
            task_manager.add(batched_task);
            sinks.push(Box::new(IcebergPriceSink::new(writer)));
        }

        if sinks.is_empty() {
            anyhow::bail!(
                "price server has no configured sinks: set `output` (S3) and/or \
                 `iceberg_settings` to a non-empty value"
            );
        }

        task_manager.add(task_manager::periodic(
            PriceGenerator::new(settings, sinks).await?,
        ));

        task_manager.start().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
