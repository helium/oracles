use crate::{
    data_session::DataSessionIngestor, heartbeats::HeartbeatDaemon, rewarder::Rewarder,
    speedtests::SpeedtestDaemon, subsciber_location::SubscriberLocationIngestor, Settings,
};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{
    file_info_poller::LookbackBehavior, file_sink, file_source, file_upload,
    heartbeat::CellHeartbeatIngestReport, mobile_subscriber::SubscriberLocationIngestReport,
    mobile_transfer::ValidDataTransferSession, speedtest::CellSpeedtestIngestReport, FileStore,
    FileType,
};

use futures_util::TryFutureExt;
use mobile_config::Client;
use price::PriceTracker;
use tokio::signal;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        let (pool, db_join_handle) = settings
            .database
            .connect(env!("CARGO_PKG_NAME"), shutdown_listener.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let ingest = FileStore::from_settings(&settings.ingest).await?;
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;

        // Heartbeats
        let (heartbeats, heartbeats_join_handle) =
            file_source::continuous_source::<CellHeartbeatIngestReport>()
                .db(pool.clone())
                .store(ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellHeartbeatIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        // Speedtests
        let (speedtests, speedtests_join_handle) =
            file_source::continuous_source::<CellSpeedtestIngestReport>()
                .db(pool.clone())
                .store(ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellSpeedtestIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        // Valid Heartbeats
        let (valid_heartbeats, mut valid_heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
            shutdown_listener.clone(),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Valid Speedtests
        let (valid_speedtests, mut valid_speedtests_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
            shutdown_listener.clone(),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Mobile rewards
        let (mobile_rewards, mut mobile_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::MobileRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_radio_reward_shares"),
            shutdown_listener.clone(),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifests, mut reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_reward_manifest"),
            shutdown_listener.clone(),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let reward_period_hours = settings.rewards;
        let config_client = Client::from_settings(&settings.config_client)?;

        let (price_tracker, tracker_process) =
            PriceTracker::start(&settings.price_tracker, shutdown_listener.clone()).await?;

        let heartbeat_daemon = HeartbeatDaemon::new(
            pool.clone(),
            config_client.clone(),
            heartbeats,
            valid_heartbeats,
        );

        let (subscriber_location_ingest, subscriber_location_ingest_join_handle) =
            file_source::continuous_source::<SubscriberLocationIngestReport>()
                .db(pool.clone())
                .store(ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::SubscriberLocationIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        let (data_session_ingest, data_session_ingest_join_handle) =
            file_source::continuous_source::<ValidDataTransferSession>()
                .db(pool.clone())
                .store(data_transfer_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::ValidDataTransferSession)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        let speedtest_daemon = SpeedtestDaemon::new(
            pool.clone(),
            config_client.clone(),
            speedtests,
            valid_speedtests,
        );

        let rewarder = Rewarder::new(
            pool.clone(),
            Duration::hours(reward_period_hours),
            Duration::minutes(settings.reward_offset_minutes),
            mobile_rewards,
            reward_manifests,
            price_tracker,
        );

        // TODO: retrieve initial carrier keys from config service
        let subscriber_location_ingestor =
            SubscriberLocationIngestor::new(pool.clone(), Vec::new(), subscriber_location_ingest);

        let data_session_ingestor = DataSessionIngestor { pool: pool.clone() };

        tokio::try_join!(
            db_join_handle.map_err(Error::from),
            valid_heartbeats_server.run().map_err(Error::from),
            valid_speedtests_server.run().map_err(Error::from),
            mobile_rewards_server.run().map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            reward_manifests_server
                .run()
                .map_err(Error::from),
            subscriber_location_ingestor
                .run(&shutdown_listener)
                .map_err(Error::from),
            data_session_ingestor
                .run(data_session_ingest, shutdown_listener.clone())
                .map_err(Error::from),
            tracker_process.map_err(Error::from),
            heartbeats_join_handle.map_err(Error::from),
            speedtests_join_handle.map_err(Error::from),
            heartbeat_daemon.run(shutdown_listener.clone()),
            speedtest_daemon.run(shutdown_listener.clone()),
            rewarder.run(shutdown_listener.clone()),
            subscriber_location_ingest_join_handle.map_err(anyhow::Error::from),
            data_session_ingest_join_handle.map_err(anyhow::Error::from),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
