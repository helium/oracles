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
        //
        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        // Create database pool
        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let (pool, db_join_handle) = settings
            .database
            .connect(&app_name, shutdown_listener.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let file_store = FileStore::from_settings(&settings.verifier).await?;

        let (receiver, source_join_handle) = file_source::continuous_source::<RewardManifest>()
            .db(pool.clone())
            .store(file_store)
            .file_type(FileType::RewardManifest)
            .lookback(LookbackBehavior::StartAfter(
                Utc.timestamp_opt(settings.start_after as i64, 0)
                    .single()
                    .unwrap(),
            ))
            .poll_duration(settings.interval())
            .offset(settings.interval() * 2)
            .build()?
            .start(shutdown_listener.clone())
            .await?;

        // Reward server
        let mut indexer = Indexer::new(settings, pool).await?;

        tokio::try_join!(
            db_join_handle.map_err(anyhow::Error::from),
            source_join_handle.map_err(anyhow::Error::from),
            indexer.run(shutdown_listener, receiver),
        )?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
