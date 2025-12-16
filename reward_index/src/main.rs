use anyhow::Result;
use clap::Parser;
use file_store::file_source;
use file_store_oracles::FileType;
use reward_index::{settings::Settings, telemetry, Indexer};
use std::path::PathBuf;
use task_manager::TaskManager;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Indexer")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
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
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
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

        // Create database pool
        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let file_store_client = settings.file_store.connect().await;
        let (receiver, server) = file_source::continuous_source()
            .state(pool.clone())
            .file_store(file_store_client.clone(), settings.input_bucket.clone())
            .prefix(FileType::RewardManifest.to_string())
            .lookback_start_after(settings.start_after)
            .poll_duration(settings.interval)
            .offset(settings.interval * 2)
            .create()
            .await?;

        // Reward server
        let indexer = Indexer::from_settings(
            settings,
            pool,
            file_store_client,
            settings.input_bucket.clone(),
            receiver,
        )
        .await?;

        TaskManager::builder()
            .add_task(server)
            .add_task(indexer)
            .build()
            .start()
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
