use std::collections::HashMap;

use crate::{
    boosting_oracles::{FootfallData, UrbanizationData},
    coverage::CoverageDaemon,
    data_session::DataSessionIngestor,
    geofence::Geofence,
    heartbeats::cbrs::HeartbeatDaemon as CellHeartbeatDaemon,
    heartbeats::wifi::HeartbeatDaemon as WifiHeartbeatDaemon,
    invalidated_radio_threshold::InvalidatedRadioThresholdIngestor,
    radio_threshold::RadioThresholdIngestor,
    rewarder::Rewarder,
    speedtests::SpeedtestDaemon,
    subscriber_location::SubscriberLocationIngestor,
    telemetry, Settings,
};
use anyhow::Result;
use chrono::Duration;
use file_store::{
    coverage::CoverageObjectIngestReport, file_info_poller::LookbackBehavior, file_sink,
    file_source, file_upload, heartbeat::CbrsHeartbeatIngestReport,
    mobile_radio_invalidated_threshold::InvalidatedRadioThresholdIngestReport,
    mobile_radio_threshold::RadioThresholdIngestReport,
    mobile_subscriber::SubscriberLocationIngestReport, mobile_transfer::ValidDataTransferSession,
    speedtest::CellSpeedtestIngestReport, wifi_heartbeat::WifiHeartbeatIngestReport, FileStore,
    FileType,
};
use hextree::disktree::DiskTreeMap;
use mobile_config::client::{
    entity_client::EntityClient, hex_boosting_client::HexBoostingClient, AuthorizationClient,
    CarrierServiceClient, GatewayClient,
};
use price::PriceTracker;
use task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let report_ingest = FileStore::from_settings(&settings.ingest).await?;
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;

        // mobile config clients
        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;
        let entity_client = EntityClient::from_settings(&settings.config_client)?;
        let carrier_client = CarrierServiceClient::from_settings(&settings.config_client)?;
        let hex_boosting_client = HexBoostingClient::from_settings(&settings.config_client)?;

        // price tracker
        let (price_tracker, price_daemon) = PriceTracker::new_tm(&settings.price_tracker).await?;

        // CBRS Heartbeats
        let (cbrs_heartbeats, cbrs_heartbeats_server) =
            file_source::continuous_source::<CbrsHeartbeatIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CbrsHeartbeatIngestReport.to_string())
                .queue_size(1)
                .create()
                .await?;

        // Wifi Heartbeats
        let (wifi_heartbeats, wifi_heartbeats_server) =
            file_source::continuous_source::<WifiHeartbeatIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::WifiHeartbeatIngestReport.to_string())
                .create()
                .await?;

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

        // Seniority updates
        let (seniority_updates, seniority_updates_server) = file_sink::FileSinkBuilder::new(
            FileType::SeniorityUpdate,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_seniority_update"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let usa_region_paths = settings.usa_region_paths()?;
        tracing::info!(?usa_region_paths, "usa_geofence_regions");

        let usa_geofence = Geofence::new(usa_region_paths, settings.usa_fencing_resolution()?)?;

        let cbrs_heartbeat_daemon = CellHeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            cbrs_heartbeats,
            settings.modeled_coverage_start(),
            settings.max_asserted_distance_deviation,
            settings.max_distance_from_coverage,
            valid_heartbeats.clone(),
            seniority_updates.clone(),
            usa_geofence.clone(),
        );

        let usa_and_mexico_region_paths = settings.usa_and_mexico_region_paths()?;
        tracing::info!(
            ?usa_and_mexico_region_paths,
            "usa_and_mexico_geofence_regions"
        );

        let usa_and_mexico_geofence = Geofence::new(
            usa_and_mexico_region_paths,
            settings.usa_and_mexico_fencing_resolution()?,
        )?;

        let wifi_heartbeat_daemon = WifiHeartbeatDaemon::new(
            pool.clone(),
            gateway_client.clone(),
            wifi_heartbeats,
            settings.modeled_coverage_start(),
            settings.max_asserted_distance_deviation,
            settings.max_distance_from_coverage,
            valid_heartbeats,
            seniority_updates,
            usa_and_mexico_geofence,
        );

        // Speedtests
        let (speedtests, speedtests_server) =
            file_source::continuous_source::<CellSpeedtestIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CellSpeedtestIngestReport.to_string())
                .create()
                .await?;

        let (speedtests_avg, speedtests_avg_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let (speedtests_validity, speedtests_validity_server) = file_sink::FileSinkBuilder::new(
            FileType::VerifiedSpeedtest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_verified_speedtest"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let speedtest_daemon = SpeedtestDaemon::new(
            pool.clone(),
            gateway_client,
            speedtests,
            speedtests_avg.clone(),
            speedtests_validity,
        );

        // Coverage objects
        let (coverage_objs, coverage_objs_server) =
            file_source::continuous_source::<CoverageObjectIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::CoverageObjectIngestReport.to_string())
                .create()
                .await?;

        let (valid_coverage_objs, valid_coverage_objs_server) = file_sink::FileSinkBuilder::new(
            FileType::CoverageObject,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_coverage_object"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Oracle boosting reports
        let (oracle_boosting_reports, oracle_boosting_reports_server) =
            file_sink::FileSinkBuilder::new(
                FileType::OracleBoostingReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_oracle_boosting_report"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .roll_time(Duration::minutes(15))
            .create()
            .await?;

        let disktree = DiskTreeMap::open(&settings.urbanization_data_set)?;
        let urbanization_data = UrbanizationData::new(disktree, usa_geofence);
        // TODO: get real data
        let temp_footfall_data = HashMap::default();
        let footfall_data = FootfallData::new(temp_footfall_data);

        let coverage_daemon = CoverageDaemon::new(
            pool.clone(),
            auth_client.clone(),
            footfall_data,
            urbanization_data,
            coverage_objs,
            valid_coverage_objs,
            oracle_boosting_reports,
        )
        .await?;

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
            carrier_client,
            hex_boosting_client,
            Duration::hours(reward_period_hours),
            Duration::minutes(settings.reward_offset_minutes),
            mobile_rewards,
            reward_manifests,
            price_tracker,
            speedtests_avg,
        );

        // subscriber location
        let (subscriber_location_ingest, subscriber_location_ingest_server) =
            file_source::continuous_source::<SubscriberLocationIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::SubscriberLocationIngestReport.to_string())
                .create()
                .await?;

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
            entity_client,
            subscriber_location_ingest,
            verified_subscriber_location,
        );

        // radio threshold reports
        let (radio_threshold_ingest, radio_threshold_ingest_server) =
            file_source::continuous_source::<RadioThresholdIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::RadioThresholdIngestReport.to_string())
                .create()
                .await?;

        let (verified_radio_threshold, verified_radio_threshold_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedRadioThresholdIngestReport,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_verified_radio_threshold"),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let radio_threshold_ingestor = RadioThresholdIngestor::new(
            pool.clone(),
            radio_threshold_ingest,
            verified_radio_threshold,
            auth_client.clone(),
        );

        // invalidated radio threshold reports
        let (invalidated_radio_threshold_ingest, invalidated_radio_threshold_ingest_server) =
            file_source::continuous_source::<InvalidatedRadioThresholdIngestReport, _>()
                .state(pool.clone())
                .store(report_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::InvalidatedRadioThresholdIngestReport.to_string())
                .create()
                .await?;

        let (verified_invalidated_radio_threshold, verified_invalidated_radio_threshold_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedInvalidatedRadioThresholdIngestReport,
                store_base_path,
                concat!(
                    env!("CARGO_PKG_NAME"),
                    "_verified_invalidated_radio_threshold"
                ),
            )
            .file_upload(Some(file_upload.clone()))
            .auto_commit(false)
            .create()
            .await?;

        let invalidated_radio_threshold_ingestor = InvalidatedRadioThresholdIngestor::new(
            pool.clone(),
            invalidated_radio_threshold_ingest,
            verified_invalidated_radio_threshold,
            auth_client.clone(),
        );

        // data transfers
        let (data_session_ingest, data_session_ingest_server) =
            file_source::continuous_source::<ValidDataTransferSession, _>()
                .state(pool.clone())
                .store(data_transfer_ingest.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .prefix(FileType::ValidDataTransferSession.to_string())
                .create()
                .await?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone(), data_session_ingest);

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(cbrs_heartbeats_server)
            .add_task(wifi_heartbeats_server)
            .add_task(valid_heartbeats_server)
            .add_task(speedtests_avg_server)
            .add_task(speedtests_validity_server)
            .add_task(valid_coverage_objs_server)
            .add_task(seniority_updates_server)
            .add_task(mobile_rewards_server)
            .add_task(reward_manifests_server)
            .add_task(verified_subscriber_location_server)
            .add_task(subscriber_location_ingestor)
            .add_task(radio_threshold_ingestor)
            .add_task(verified_radio_threshold_server)
            .add_task(invalidated_radio_threshold_ingestor)
            .add_task(verified_invalidated_radio_threshold_server)
            .add_task(data_session_ingest_server)
            .add_task(price_daemon)
            .add_task(cbrs_heartbeat_daemon)
            .add_task(wifi_heartbeat_daemon)
            .add_task(speedtests_server)
            .add_task(coverage_objs_server)
            .add_task(oracle_boosting_reports_server)
            .add_task(speedtest_daemon)
            .add_task(coverage_daemon)
            .add_task(rewarder)
            .add_task(subscriber_location_ingest_server)
            .add_task(radio_threshold_ingest_server)
            .add_task(invalidated_radio_threshold_ingest_server)
            .add_task(data_session_ingestor)
            .start()
            .await
    }
}
