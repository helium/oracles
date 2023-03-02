use anyhow::Result;
use chrono::{TimeZone, Utc};
use clap::Parser;
use file_store::{
    file_info_poller::LookbackBehavior, file_source, reward_manifest::RewardManifest, FileStore,
    FileType,
};
use futures_util::TryFutureExt;
use reward_index::{settings::Settings, Indexer};
use std::path::PathBuf;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Indexer")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
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

        // Create database pool
        let pool = settings.database.connect(10).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let file_store = FileStore::from_settings(&settings.verifier).await?;

        let (receiver, source_join_handle) = file_source::continuous_source::<RewardManifest>()
            .db(pool)
            .store(file_store)
            .file_type(FileType::RewardManifest)
            .lookback(LookbackBehavior::StartAfter(
                Utc.timestamp_opt(0, 0).single().unwrap(),
            ))
            .poll_duration(settings.interval())
            .offset(settings.interval() * 2)
            .build()?
            .start(shutdown_listener.clone())
            .await?;

        // Reward server
        let mut indexer = Indexer::new(settings).await?;

        tokio::try_join!(
            indexer.run(shutdown_listener.clone(), receiver),
            source_join_handle.map_err(anyhow::Error::from),
        )?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
