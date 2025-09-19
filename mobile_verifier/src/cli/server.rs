use std::time::Duration;

use crate::{
    banning,
    boosting_oracles::DataSetDownloaderDaemon,
    coverage::{new_coverage_object_notification_channel, CoverageDaemon},
    data_session::DataSessionIngestor,
    geofence::Geofence,
    heartbeats::wifi::WifiHeartbeatDaemon,
    rewarder::Rewarder,
    speedtests::SpeedtestDaemon,
    subscriber_mapping_activity::SubscriberMappingActivityDaemon,
    telemetry,
    unique_connections::ingestor::UniqueConnectionsIngestor,
    Settings,
};
use anyhow::Result;
use file_store::{
    file_upload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
};
use helium_proto::services::poc_mobile::{Heartbeat, SeniorityUpdate, SpeedtestAvg};
use mobile_config::client::{
    entity_client::EntityClient, hex_boosting_client::HexBoostingClient,
    sub_dao_client::SubDaoClient, AuthorizationClient, CarrierServiceClient, GatewayClient,
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

        let file_store_client = settings.file_store.connect().await;
        let (file_upload, file_upload_server) = file_upload::FileUpload::new(
            file_store_client.clone(),
            settings.buckets.output.clone(),
        )
        .await;

        // mobile config clients
        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;
        let entity_client = EntityClient::from_settings(&settings.config_client)?;
        let carrier_client = CarrierServiceClient::from_settings(&settings.config_client)?;
        let hex_boosting_client = HexBoostingClient::from_settings(&settings.config_client)?;
        let sub_dao_rewards_client = SubDaoClient::from_settings(&settings.config_client)?;

        let (valid_heartbeats, valid_heartbeats_server) = Heartbeat::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        // Seniority updates
        let (seniority_updates, seniority_updates_server) = SeniorityUpdate::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (speedtests_avg, speedtests_avg_server) = SpeedtestAvg::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

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
                WifiHeartbeatDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
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
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
                    speedtests_avg.clone(),
                    gateway_client.clone(),
                )
                .await?,
            )
            .add_task(
                SubscriberMappingActivityDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    auth_client.clone(),
                    entity_client.clone(),
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
                    file_upload.clone(),
                )
                .await?,
            )
            .add_task(
                CoverageDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
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
                    file_store_client.clone(),
                    settings.buckets.data_sets.clone(),
                    new_coverage_obj_notification,
                )
                .await?,
            )
            .add_task(
                UniqueConnectionsIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
                    auth_client.clone(),
                )
                .await?,
            )
            .add_task(
                DataSessionIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    file_store_client.clone(),
                    settings.buckets.data_transfer.clone(),
                )
                .await?,
            )
            .add_task(
                banning::create_managed_task(
                    pool.clone(),
                    file_upload.clone(),
                    file_store_client.clone(),
                    settings.buckets.ingest.clone(),
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
                    file_store_client.clone(),
                    carrier_client,
                    hex_boosting_client,
                    sub_dao_rewards_client,
                    speedtests_avg,
                )
                .await?,
            )
            .build()
            .start()
            .await
    }
}
