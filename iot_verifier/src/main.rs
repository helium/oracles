use anyhow::{Error, Result};
use clap::Parser;
use density_scaler::Server as DensityScaler;
use file_store::{file_sink, file_upload, FileType};
use futures::TryFutureExt;
use iot_verifier::{
    entropy_loader, gateway_cache::GatewayCache, loader, metrics::Metrics, poc_report::Report,
    purger, rewarder::Rewarder, runner, Settings,
};
use std::path;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium POC IOT Verifier")]
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

        // Create database pool and run migrations
        let pool = settings.database.connect(2).await?;
        sqlx::migrate!().run(&pool).await?;

        let count_all_beacons = Report::count_all_beacons(&pool).await?;
        Metrics::num_beacons(count_all_beacons);

        // configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let gateway_cache = GatewayCache::from_settings(settings);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);
        // Gateway reward shares sink
        let (gateway_rewards_tx, gateway_rewards_rx) = file_sink::message_channel(50);
        let mut gateway_rewards = file_sink::FileSinkBuilder::new(
            FileType::GatewayRewardShare,
            store_base_path,
            gateway_rewards_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifest_tx, reward_manifest_rx) = file_sink::message_channel(50);
        let mut reward_manifests = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            reward_manifest_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let rewarder = Rewarder {
            pool,
            gateway_rewards_tx,
            reward_manifest_tx,
            reward_period_hours: settings.rewards,
            reward_offset: settings.reward_offset_duration(),
        };

        let mut loader = loader::Loader::from_settings(settings).await?;
        let mut entropy_loader = entropy_loader::EntropyLoader::from_settings(settings).await?;
        let mut runner = runner::Runner::from_settings(settings).await?;
        let purger = purger::Purger::from_settings(settings).await?;
        let mut density_scaler =
            DensityScaler::from_settings(settings.density_scaler.clone()).await?;
        tokio::try_join!(
            gateway_rewards.run(&shutdown).map_err(Error::from),
            reward_manifests.run(&shutdown).map_err(Error::from),
            file_upload.run(&shutdown).map_err(Error::from),
            runner.run(
                file_upload_tx.clone(),
                &gateway_cache,
                density_scaler.hex_density_map(),
                &shutdown
            ),
            entropy_loader.run(&shutdown),
            loader.run(&shutdown, &gateway_cache),
            purger.run(&shutdown),
            rewarder.run(&shutdown),
            density_scaler.run(&shutdown).map_err(Error::from),
        )
        .map(|_| ())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
