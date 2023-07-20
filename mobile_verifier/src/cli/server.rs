use crate::{
    data_session::DataSessionIngestor, heartbeats::HeartbeatDaemon, rewarder::Rewarder,
    speedtests::SpeedtestDaemon, subscriber_location::SubscriberLocationIngestor, telemetry,
    Settings,
};
use anyhow::Result;
use chrono::Duration;
use file_store::{
    file_info_poller::LookbackBehavior, file_sink, file_source, file_upload,
    heartbeat::CellHeartbeatIngestReport, mobile_subscriber::SubscriberLocationIngestReport,
    mobile_transfer::ValidDataTransferSession, speedtest::CellSpeedtestIngestReport, FileStore,
    FileType,
};

use mobile_config::client::{AuthorizationClient, EntityClient, GatewayClient};
use price::PriceTracker;
use task_manager::TaskManager;
use tokio::signal;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let report_ingest = FileStore::from_settings(&settings.ingest).await?;
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;

        // mobile config clients
        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;
        let entity_client = EntityClient::from_settings(&settings.config_client)?;

        // todo: update price tracker to not require shutdown listener to be passed in
        // price tracker
        let (shutdown_trigger, shutdown) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });
        let (price_tracker, _price_receiver) =
            PriceTracker::start(&settings.price_tracker, shutdown.clone()).await?;

        // Heartbeats
        let (heartbeats, heartbeats_ingest_server) =
            file_source::continuous_source::<CellHeartbeatIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellHeartbeatIngestReport)
                .create()?;

        let (valid_heartbeats, valid_heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let heartbeat_daemon = HeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            heartbeats,
            valid_heartbeats,
        );

        // Speedtests
        let (speedtests, speedtests_ingest_server) =
            file_source::continuous_source::<CellSpeedtestIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellSpeedtestIngestReport)
                .create()?;

        let (valid_speedtests, valid_speedtests_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let speedtest_daemon = SpeedtestDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            speedtests,
            valid_speedtests,
        );

        // Mobile rewards
        let reward_period_hours = settings.rewards;
        let (mobile_rewards, mobile_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::MobileRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_radio_reward_shares"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let (reward_manifests, reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_reward_manifest"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let rewarder = Rewarder::new(
            pool.clone(),
            Duration::hours(reward_period_hours),
            Duration::minutes(settings.reward_offset_minutes),
            mobile_rewards,
            reward_manifests,
            price_tracker,
        );

        // subscriber location
        let (subscriber_location_ingest, subscriber_location_ingest_server) =
            file_source::continuous_source::<SubscriberLocationIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::SubscriberLocationIngestReport)
                .create()?;

        let (verified_subscriber_location, verified_subscriber_location_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedSubscriberLocationIngestReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_verified_subscriber_location"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let subscriber_location_ingestor = SubscriberLocationIngestor::new(
            pool.clone(),
            auth_client.clone(),
            entity_client.clone(),
            subscriber_location_ingest,
            verified_subscriber_location,
        );

        // data transfers
        let (data_session_ingest, data_session_ingest_server) =
            file_source::continuous_source::<ValidDataTransferSession>()
                .db(pool.clone())
                .store(data_transfer_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::ValidDataTransferSession)
                .create()?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone(), data_session_ingest);

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_heartbeats_server)
            .add_task(valid_speedtests_server)
            .add_task(mobile_rewards_server)
            .add_task(reward_manifests_server)
            .add_task(verified_subscriber_location_server)
            .add_task(heartbeats_ingest_server)
            .add_task(speedtests_ingest_server)
            .add_task(subscriber_location_ingest_server)
            .add_task(data_session_ingest_server)
            .add_task(subscriber_location_ingestor)
            .add_task(data_session_ingestor)
            .add_task(heartbeat_daemon)
            .add_task(speedtest_daemon)
            .add_task(rewarder)
            .start()
            .await
    }
}
