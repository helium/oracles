use anyhow::{Error, Result};
use clap::Parser;
use file_store::{file_sink, file_upload, FileType};
use futures::TryFutureExt;
use iot_verifier::{
    entropy_loader, gateway_cache::GatewayCache, loader, metrics::Metrics, poc_report::Report,
    purger, rewarder::Rewarder, runner, tx_scaler::Server as DensityScaler, Settings,
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

        // configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Create database pool and run migrations
        let (pool, db_join_handle) = settings
            .database
            .connect(env!("CARGO_PKG_NAME"), shutdown.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let count_all_beacons = Report::count_all_beacons(&pool).await?;
        Metrics::num_beacons(count_all_beacons);

        let gateway_cache = GatewayCache::from_settings(settings);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);
        // Gateway reward shares sink
        let (gateway_rewards_sink, mut gateway_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::GatewayRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_gateway_reward_shares"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifests_sink, mut reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_iot_reward_manifest"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let rewarder = Rewarder {
            pool: pool.clone(),
            gateway_rewards_sink,
            reward_manifests_sink,
            reward_period_hours: settings.rewards,
            reward_offset: settings.reward_offset_duration(),
        };

        let mut loader = loader::Loader::from_settings(settings, pool.clone()).await?;
        let mut entropy_loader =
            entropy_loader::EntropyLoader::from_settings(settings, pool.clone()).await?;
        let mut runner = runner::Runner::from_settings(settings, pool.clone()).await?;
        let purger = purger::Purger::from_settings(settings, pool.clone()).await?;
        let mut density_scaler = DensityScaler::from_settings(settings, pool).await?;
        tokio::try_join!(
            db_join_handle.map_err(Error::from),
            gateway_rewards_server.run(&shutdown).map_err(Error::from),
            reward_manifests_server.run(&shutdown).map_err(Error::from),
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
