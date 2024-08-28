use std::time::Duration;

use crate::{
    boosting_oracles::DataSetDownloaderDaemon,
    coverage::{new_coverage_object_notification_channel, CoverageDaemon},
    data_session::DataSessionIngestor,
    geofence::Geofence,
    heartbeats::{cbrs::CbrsHeartbeatDaemon, wifi::WifiHeartbeatDaemon},
    promotion_reward::PromotionRewardDaemon,
    radio_threshold::RadioThresholdIngestor,
    rewarder::Rewarder,
    sp_boosted_rewards_bans::ServiceProviderBoostedRewardsBanIngestor,
    speedtests::SpeedtestDaemon,
    subscriber_location::SubscriberLocationIngestor,
    subscriber_verified_mapping_event::SubscriberVerifiedMappingEventDaemon,
    telemetry, Settings,
};
use anyhow::Result;
use file_store::{
    file_upload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore,
};
use helium_proto::services::poc_mobile::{Heartbeat, SeniorityUpdate, SpeedtestAvg};
use mobile_config::client::{
    entity_client::EntityClient, hex_boosting_client::HexBoostingClient, AuthorizationClient,
    CarrierServiceClient, GatewayClient,
};
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

        // mobile config clients
        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;
        let entity_client = EntityClient::from_settings(&settings.config_client)?;
        let carrier_client = CarrierServiceClient::from_settings(&settings.config_client)?;
        let hex_boosting_client = HexBoostingClient::from_settings(&settings.config_client)?;

        let (valid_heartbeats, valid_heartbeats_server) = Heartbeat::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        // Seniority updates
        let (seniority_updates, seniority_updates_server) = SeniorityUpdate::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (speedtests_avg, speedtests_avg_server) = SpeedtestAvg::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let usa_region_paths = settings.usa_region_paths()?;
        tracing::info!(?usa_region_paths, "usa_geofence_regions");

        let usa_geofence =
            Geofence::from_paths(usa_region_paths, settings.usa_fencing_resolution()?)?;

        let usa_and_mexico_region_paths = settings.usa_and_mexico_region_paths()?;
        tracing::info!(
            ?usa_and_mexico_region_paths,
            "usa_and_mexico_geofence_regions"
        );

        let usa_and_mexico_geofence = Geofence::from_paths(
            usa_and_mexico_region_paths,
            settings.usa_and_mexico_fencing_resolution()?,
        )?;

        let (new_coverage_obj_notifier, new_coverage_obj_notification) =
            new_coverage_object_notification_channel();

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_heartbeats_server)
            .add_task(seniority_updates_server)
            .add_task(speedtests_avg_server)
            .add_task(
                CbrsHeartbeatDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    report_ingest.clone(),
                    gateway_client.clone(),
                    valid_heartbeats.clone(),
                    seniority_updates.clone(),
                    usa_geofence,
                )
                .await?,
            )
            .add_task(
                WifiHeartbeatDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    report_ingest.clone(),
                    gateway_client.clone(),
                    valid_heartbeats,
                    seniority_updates.clone(),
                    usa_and_mexico_geofence,
                )
                .await?,
            )
            .add_task(
                SpeedtestDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest.clone(),
                    speedtests_avg.clone(),
                    gateway_client.clone(),
                )
                .await?,
            )
            .add_task(
                SubscriberVerifiedMappingEventDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    auth_client.clone(),
                    entity_client.clone(),
                    report_ingest.clone(),
                    file_upload.clone(),
                )
                .await?,
            )
            .add_task(
                CoverageDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest.clone(),
                    auth_client.clone(),
                    new_coverage_obj_notifier,
                )
                .await?,
            )
            .add_task(
                DataSetDownloaderDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    new_coverage_obj_notification,
                )
                .await?,
            )
            .add_task(
                SubscriberLocationIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest.clone(),
                    auth_client.clone(),
                    entity_client.clone(),
                )
                .await?,
            )
            .add_task(
                RadioThresholdIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest.clone(),
                    auth_client.clone(),
                )
                .await?,
            )
            .add_task(
                PromotionRewardDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest.clone(),
                    gateway_client.clone(),
                    auth_client.clone(),
                    entity_client.clone(),
                )
                .await?,
            )
            .add_task(DataSessionIngestor::create_managed_task(pool.clone(), settings).await?)
            .add_task(
                ServiceProviderBoostedRewardsBanIngestor::create_managed_task(
                    pool.clone(),
                    file_upload.clone(),
                    report_ingest,
                    auth_client,
                    settings,
                    seniority_updates,
                )
                .await?,
            )
            .add_task(
                Rewarder::create_managed_task(
                    pool,
                    settings,
                    file_upload,
                    carrier_client,
                    hex_boosting_client,
                    speedtests_avg,
                )
                .await?,
            )
            .build()
            .start()
            .await
    }
}
