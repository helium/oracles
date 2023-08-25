use crate::entropy_loader::EntropyLoader;
use anyhow::Result;
use chrono::Duration as ChronoDuration;
use clap::Parser;
use file_store::{
    entropy_report::EntropyReport, file_info_poller::LookbackBehavior, file_sink, file_source,
    file_upload, iot_packet::IotValidPacket, FileStore, FileType,
};
use iot_config::client::Client as IotConfigClient;
use iot_verifier::{
    entropy_loader, gateway_cache::GatewayCache, gateway_updater::GatewayUpdater, loader,
    packet_loader, purger, region_cache::RegionCache, rewarder::Rewarder, runner, telemetry,
    tx_scaler::Server as DensityScaler, Settings,
};
use price::PriceTracker;
use std::path;
use task_manager::TaskManager;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium POC IOT Verifier")]

pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
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
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;
        let store_base_path = path::Path::new(&settings.cache);

        let iot_config_client = IotConfigClient::from_settings(&settings.iot_config_client)?;

        // *
        // setup caches
        // *
        let (gateway_updater_receiver, gateway_updater_server) =
            GatewayUpdater::from_settings(settings, iot_config_client.clone()).await?;
        let gateway_cache = GatewayCache::new(gateway_updater_receiver.clone());
        let region_cache = RegionCache::from_settings(settings, iot_config_client)?;

        // *
        // setup the price tracker requirements
        // *
        let (price_tracker, price_daemon) = PriceTracker::new_tm(&settings.price_tracker).await?;

        // *
        // setup the loader requirements
        // *
        let loader =
            loader::Loader::from_settings(settings, pool.clone(), gateway_cache.clone()).await?;

        // *
        // setup the density scaler requirements
        // *
        let density_scaler =
            DensityScaler::from_settings(settings, pool.clone(), gateway_updater_receiver).await?;

        // *
        // setup the rewarder requirements
        // *

        // Gateway reward shares sink
        let (rewards_sink, gateway_rewards_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_gateway_reward_shares"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifests_sink, reward_manifests_sink_server) =
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
                .store(entropy_store)
                .file_type(FileType::EntropyReport)
                .lookback(LookbackBehavior::Max(max_lookback_age))
                .poll_duration(entropy_interval)
                .offset(entropy_interval * 2)
                .create()?;

        let entropy_loader = EntropyLoader {
            pool: pool.clone(),
            file_receiver: entropy_loader_receiver,
        };

        // *
        // setup the packet loader requirements
        // *

        let (non_rewardable_packet_sink, non_rewardable_packet_sink_server) =
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
                .create()?;

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

        let (runner_poc_sink, runner_poc_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::IotPoc,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_valid_poc"),
        )
        .file_upload(Some(file_upload.clone()))
        .roll_time(ChronoDuration::minutes(2))
        .create()
        .await?;

        let runner = runner::Runner::from_settings(
            settings,
            pool.clone(),
            gateway_cache.clone(), // todo: confirm this is just cloning a reference to an arc
            region_cache.clone(),  // todo: confirm this is just cloning a reference to an arc
            runner_invalid_beacon_sink,
            runner_invalid_witness_sink,
            runner_poc_sink,
            density_scaler.hex_density_map.clone(), // todo: confirm this is just cloning a reference to an arc
        )
        .await?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(gateway_rewards_sink_server)
            .add_task(reward_manifests_sink_server)
            .add_task(non_rewardable_packet_sink_server)
            .add_task(purger_invalid_beacon_sink_server)
            .add_task(purger_invalid_witness_sink_server)
            .add_task(runner_invalid_beacon_sink_server)
            .add_task(runner_invalid_witness_sink_server)
            .add_task(runner_poc_sink_server)
            .add_task(price_daemon)
            .add_task(density_scaler)
            .add_task(gateway_updater_server)
            .add_task(purger)
            .add_task(runner)
            .add_task(entropy_loader)
            .add_task(packet_loader)
            .add_task(loader)
            .add_task(pk_loader_server)
            .add_task(entropy_loader_server)
            .add_task(rewarder)
            .start()
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
