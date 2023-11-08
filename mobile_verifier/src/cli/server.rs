use crate::{
    coverage::CoverageDaemon, data_session::DataSessionIngestor,
    heartbeats::cbrs::HeartbeatDaemon as CellHeartbeatDaemon,
    heartbeats::wifi::HeartbeatDaemon as WifiHeartbeatDaemon, rewarder::Rewarder,
    speedtests::SpeedtestDaemon, subscriber_location::SubscriberLocationIngestor, telemetry,
    Settings,
};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{
    coverage::CoverageObjectIngestReport, file_info_poller::LookbackBehavior, file_sink,
    file_source, file_upload, heartbeat::CbrsHeartbeatIngestReport,
    mobile_subscriber::SubscriberLocationIngestReport, mobile_transfer::ValidDataTransferSession,
    speedtest::CellSpeedtestIngestReport, wifi_heartbeat::WifiHeartbeatIngestReport, FileStore,
    FileType,
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

        // CBRS Heartbeats
        let (cbrs_heartbeats, cbrs_heartbeats_server) =
            file_source::continuous_source::<CbrsHeartbeatIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CbrsHeartbeatIngestReport.to_string())
                .create()?;
        let cbrs_heartbeats_join_handle = cbrs_heartbeats_server
            .start(shutdown_listener.clone())
            .await?;

        // Wifi Heartbeats
        let (wifi_heartbeats, wifi_heartbeats_server) =
            file_source::continuous_source::<WifiHeartbeatIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::WifiHeartbeatIngestReport.to_string())
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

        // Seniority updates
        let (seniority_updates, seniority_updates_server) = file_sink::FileSinkBuilder::new(
            FileType::SeniorityUpdate,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_seniority_update"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let cbrs_heartbeat_daemon = CellHeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            cbrs_heartbeats,
            settings.max_heartbeat_distance_from_coverage_km,
            settings.modeled_coverage_start(),
            valid_heartbeats.clone(),
            seniority_updates.clone(),
        );

        let wifi_heartbeat_daemon = WifiHeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            wifi_heartbeats,
            settings.max_heartbeat_distance_from_coverage_km,
            settings.modeled_coverage_start(),
            valid_heartbeats,
            seniority_updates,
        );

        // Speedtests
        let (speedtests, speedtests_server) =
            file_source::continuous_source::<CellSpeedtestIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CellSpeedtestIngestReport.to_string())
                .create()?;
        let speedtests_join_handle = speedtests_server.start(shutdown_listener.clone()).await?;

        let (speedtests_avg, speedtests_avg_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let (speedtests_validity, speedtests_validity_server) = file_sink::FileSinkBuilder::new(
            FileType::VerifiedSpeedtest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_verified_speedtest"),
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
            speedtests_avg,
            speedtests_validity,
        );

        // Coverage objects
        let (coverage_objs, coverage_objs_server) =
            file_source::continuous_source::<CoverageObjectIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CoverageObjectIngestReport.to_string())
                .create()?;
        let coverage_objs_join_handle = coverage_objs_server
            .start(shutdown_listener.clone())
            .await?;

        let (valid_coverage_objs, valid_coverage_objs_server) = file_sink::FileSinkBuilder::new(
            FileType::CoverageObject,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_coverage_object"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let coverage_daemon = CoverageDaemon::new(
            pool.clone(),
            auth_client.clone(),
            coverage_objs,
            valid_coverage_objs,
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
            settings.max_asserted_distance_deviation,
        );

        // subscriber location
        let (subscriber_location_ingest, subscriber_location_ingest_server) =
            file_source::continuous_source::<SubscriberLocationIngestReport>()
                .db(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::SubscriberLocationIngestReport.to_string())
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
                .prefix(FileType::ValidDataTransferSession.to_string())
                .create()?;
        let data_session_ingest_join_handle = data_session_ingest_server
            .start(shutdown_listener.clone())
            .await?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone());

        tokio::try_join!(
            valid_heartbeats_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            speedtests_avg_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            speedtests_validity_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            valid_coverage_objs_server
                .run(shutdown_listener.clone())
                .map_err(Error::from),
            seniority_updates_server
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
            cbrs_heartbeats_join_handle.map_err(Error::from),
            wifi_heartbeats_join_handle.map_err(Error::from),
            speedtests_join_handle.map_err(Error::from),
            coverage_objs_join_handle.map_err(Error::from),
            cbrs_heartbeat_daemon.run(shutdown_listener.clone()),
            wifi_heartbeat_daemon.run(shutdown_listener.clone()),
            speedtest_daemon.run(shutdown_listener.clone()),
            coverage_daemon.run(shutdown_listener.clone()),
            rewarder.run(shutdown_listener.clone()),
            subscriber_location_ingest_join_handle.map_err(anyhow::Error::from),
            data_session_ingest_join_handle.map_err(anyhow::Error::from),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
