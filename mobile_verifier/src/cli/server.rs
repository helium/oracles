use crate::{
    data_session::DataSessionIngestor, heartbeats::HeartbeatDaemon, rewarder::Rewarder,
    speedtests::SpeedtestDaemon, subscriber_location::SubscriberLocationIngestor, telemetry,
    Settings,
};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{
    file_info_poller::LookbackBehavior, file_sink, file_source, file_upload,
    heartbeat::CellHeartbeatIngestReport, mobile_subscriber::SubscriberLocationIngestReport,
    mobile_transfer::ValidDataTransferSession, speedtest::CellSpeedtestIngestReport,
    wifi_heartbeat::WifiHeartbeatIngestReport, FileStore, FileType,
};

use futures_util::TryFutureExt;
use mobile_config::client::{AuthorizationClient, EntityClient, GatewayClient};
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

        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let report_ingest = FileStore::from_settings(&settings.ingest).await?;
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;

        // mobile config clients
        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;
        let entity_client = EntityClient::from_settings(&settings.config_client)?;

        // price tracker
        let (price_tracker, tracker_process) =
            PriceTracker::start(&settings.price_tracker, shutdown_listener.clone()).await?;

        // Cell Heartbeats
        let (heartbeats, heartbeats_server) =
            file_source::continuous_source::<CellHeartbeatIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellHeartbeatIngestReport)
                .create()?;
        let heartbeats_join_handle = heartbeats_server.start(shutdown_listener.clone()).await?;

        // Wifi Heartbeats
        let (wifi_heartbeats, wifi_heartbeats_server) =
            file_source::continuous_source::<WifiHeartbeatIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::WifiHeartbeatIngestReport)
                .create()?;
        let wifi_heartbeats_join_handle = wifi_heartbeats_server
            .start(shutdown_listener.clone())
            .await?;

        let (valid_heartbeats, valid_heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let heartbeat_daemon = HeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            heartbeats,
            wifi_heartbeats,
            valid_heartbeats,
        );

        // Speedtests
        let (speedtests, speedtests_server) =
            file_source::continuous_source::<CellSpeedtestIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::CellSpeedtestIngestReport)
                .create()?;
        let speedtests_join_handle = speedtests_server.start(shutdown_listener.clone()).await?;

        let (valid_speedtests, valid_speedtests_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .deposits(Some(file_upload_tx.clone()))
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
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let (reward_manifests, reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_reward_manifest"),
        )
        .deposits(Some(file_upload_tx.clone()))
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
        let subscriber_location_ingest_join_handle = subscriber_location_ingest_server
            .start(shutdown_listener.clone())
            .await?;

        let (verified_subscriber_location, verified_subscriber_location_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedSubscriberLocationIngestReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_verified_subscriber_location"),
            )
            .deposits(Some(file_upload_tx.clone()))
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
        let data_session_ingest_join_handle = data_session_ingest_server
            .start(shutdown_listener.clone())
            .await?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone());

        tokio::try_join!(
            valid_heartbeats_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            valid_speedtests_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            mobile_rewards_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            file_upload
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            reward_manifests_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            verified_subscriber_location_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            subscriber_location_ingestor
                .run(&shutdown_listener)
                .map_err(Error::from),
            data_session_ingestor
                .run(data_session_ingest, shutdown_listener.clone())
                .map_err(Error::from),
            tracker_process.map_err(Error::from),
            heartbeats_join_handle.map_err(Error::from),
            wifi_heartbeats_join_handle.map_err(Error::from),
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
