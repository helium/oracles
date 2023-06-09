use crate::entropy_loader::EntropyLoader;
use anyhow::{Error, Result};
use clap::Parser;
use file_store::{
    entropy_report::EntropyReport, file_info_poller::LookbackBehavior, file_sink, file_source,
    file_upload, iot_packet::IotValidPacket, FileStore, FileType,
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use iot_config::client::Client as IotConfigClient;
use iot_verifier::{
    entropy_loader, gateway_cache::GatewayCache, gateway_updater::GatewayUpdater, loader,
    metrics::Metrics, packet_loader, poc_report::Report, purger, region_cache::RegionCache,
    rewarder::Rewarder, runner, tx_scaler::Server as DensityScaler, Settings,
};
use price::PriceTracker;
use std::path;
use task_manager::ManagedTask;
use tokio::signal;
use tokio_util::sync::CancellationToken;
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
        let (pool, db_join_handle) = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        let count_all_beacons = Report::count_all_beacons(&pool).await?;
        Metrics::num_beacons(count_all_beacons);

        let iot_config_client = IotConfigClient::from_settings(&settings.iot_config_client)?;

        let (gateway_updater_receiver, gateway_updater) =
            GatewayUpdater::from_settings(settings, iot_config_client.clone()).await?;

        let gateway_cache = GatewayCache::new(gateway_updater_receiver.clone());
        let region_cache = RegionCache::from_settings(settings, iot_config_client.clone())?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        // *
        // setup the rewarder requirements
        // *

        // Gateway reward shares sink
        let (rewards_sink, mut gateway_rewards_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_gateway_reward_shares"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest sink
        let (reward_manifests_sink, mut reward_manifests_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::RewardManifest,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_iot_reward_manifest"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let rewarder = Rewarder {
            pool: pool.clone(),
            rewards_sink,
            reward_manifests_sink,
            reward_period_hours: settings.rewards,
            reward_offset: settings.reward_offset_duration(),
            price_tracker,
        };

        // *
        // setup entropy requirements
        // *
        let max_lookback_age = settings.loader_window_max_lookback_age();
        let entropy_store = FileStore::from_settings(&settings.entropy).await?;
        let entropy_interval = settings.entropy_interval();
        let (entropy_loader_receiver, entropy_loader_server) =
            file_source::continuous_source::<EntropyReport>()
                .db(pool.clone())
                .store(entropy_store.clone())
                .file_type(FileType::EntropyReport)
                .lookback(LookbackBehavior::Max(max_lookback_age))
                .poll_duration(entropy_interval)
                .offset(entropy_interval * 2)
                .build()?
                .start(shutdown.clone())
                .await?;
        let mut entropy_loader = EntropyLoader {
            pool: pool.clone(),
            file_receiver: entropy_loader_receiver,
        };

        // *
        // setup the packet loader requirements
        // *
        let (non_rewardable_packet_sink, mut non_rewardable_packet_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::NonRewardablePacket,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_non_rewardable_packet"),
            )
            .file_upload(Some(file_upload.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let packet_store = FileStore::from_settings(&settings.packet_ingest).await?;
        let packet_interval = settings.packet_interval();
        let (pk_loader_receiver, pk_loader_server) =
            file_source::continuous_source::<IotValidPacket>()
                .db(pool.clone())
                .store(packet_store.clone())
                .file_type(FileType::IotValidPacket)
                .lookback(LookbackBehavior::Max(max_lookback_age))
                .poll_duration(packet_interval)
                .offset(packet_interval * 2)
                .build()?
                .start(shutdown.clone())
                .await?;
        let packet_loader = packet_loader::PacketLoader::from_settings(
            settings,
            pool.clone(),
            gateway_cache.clone(),
            pk_loader_receiver,
            non_rewardable_packet_sink,
        );

        // *
        // setup the purger requirements
        // *
        let (purger_invalid_beacon_sink, purger_invalid_beacon_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidBeaconReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_beacon"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let (purger_invalid_witness_sink, purger_invalid_witness_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidWitnessReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_witness_report"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;
        let purger = purger::Purger::from_settings(
            settings,
            pool.clone(),
            purger_invalid_beacon_sink,
            purger_invalid_witness_sink,
        )
        .await?;

        // *
        // setup the runner requirements
        // *
        let (runner_invalid_beacon_sink, runner_invalid_beacon_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidBeaconReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_beacon_report"),
            )
            .file_upload(Some(file_upload.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let (runner_invalid_witness_sink, runner_invalid_witness_sink_server) =
            file_sink::FileSinkBuilder::new(
                FileType::IotInvalidWitnessReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_invalid_witness_report"),
            )
            .file_upload(Some(file_upload.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;

        let (runner_poc_sink, mut runner_poc_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotPoc,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_valid_poc"),
        )
        .file_upload(Some(file_upload.clone()))
        .roll_time(ChronoDuration::minutes(2))
        .create()
        .await?;

        let mut runner = runner::Runner::from_settings(
            settings,
            pool.clone(),
            gateway_cache.clone(), // TODO: fix this
            region_cache.clone(),  // TODO: fix this
            runner_invalid_beacon_sink,
            runner_invalid_witness_sink,
            runner_poc_sink,
            density_scaler.hex_density_map(), // TODO: fix this
        )
        .await?;

        // *
        // setup the loader requirements
        // *
        let mut loader =
            loader::Loader::from_settings(settings, pool.clone(), gateway_cache.clone()).await?;

        // *
        // setup the density scaler requirements
        // *
        let mut density_scaler =
            DensityScaler::from_settings(settings, pool, gateway_updater_receiver.clone()).await?;

        // *
        // setup the price tracker requirements
        // *
        let (price_tracker, price_receiver) =
            PriceTracker::start(&settings.price_tracker, shutdown.clone()).await?;

        TaskManager::builder()
            .add(file_upload_server)
            .add(gateway_rewards_sink_server)
            .add(reward_manifests_sink_server)
            .add(non_rewardable_packet_sink_server)
            .add(pk_loader_server)
            .add(purger_invalid_beacon_sink_server)
            .add(runner_invalid_beacon_sink_server)
            .add(runner_invalid_witness_sink_server)
            .add(runner_poc_sink_server)
            .add(density_scaler)
            .add(price_tracker)
            .add(entropy_loader)
            .add(packet_loader)
            .add(loader)
            .add(runner)
            .add(rewarder)
            .add(purger)
            .start()
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
