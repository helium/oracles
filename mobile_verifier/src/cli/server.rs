use crate::{
    coverage::CoverageDaemon,
    data_session::DataSessionIngestor,
    geofence::Geofence,
    heartbeats::{cbrs::CbrsHeartbeatDaemon, wifi::WifiHeartbeatDaemon},
    radio_threshold::RadioThresholdIngestor,
    rewarder::Rewarder,
    speedtests::SpeedtestDaemon,
    subscriber_location::SubscriberLocationIngestor,
    telemetry, Settings,
};
use anyhow::Result;
use chrono::Duration;
use file_store::{
    file_sink,
    file_upload::{self},
    FileStore, FileType,
};

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

        let (valid_heartbeats, valid_heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
        )
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Seniority updates
        let (seniority_updates, seniority_updates_server) = file_sink::FileSinkBuilder::new(
            FileType::SeniorityUpdate,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_seniority_update"),
        )
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let (speedtests_avg, speedtests_avg_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
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

        let mut task_manager = TaskManager::new();
        task_manager.add(file_upload_server);
        task_manager.add(valid_heartbeats_server);
        task_manager.add(seniority_updates_server);
        task_manager.add(speedtests_avg_server);

        CbrsHeartbeatDaemon::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            report_ingest.clone(),
            gateway_client.clone(),
            valid_heartbeats.clone(),
            seniority_updates.clone(),
            usa_geofence.clone(),
        )
        .await?;

        WifiHeartbeatDaemon::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            report_ingest.clone(),
            gateway_client.clone(),
            valid_heartbeats,
            seniority_updates,
            usa_and_mexico_geofence,
        )
        .await?;

        SpeedtestDaemon::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            file_upload.clone(),
            report_ingest.clone(),
            speedtests_avg.clone(),
            gateway_client,
        )
        .await?;

        CoverageDaemon::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            file_upload.clone(),
            report_ingest.clone(),
            auth_client.clone(),
            usa_geofence,
        )
        .await?;

        SubscriberLocationIngestor::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            file_upload.clone(),
            report_ingest.clone(),
            auth_client.clone(),
            entity_client,
        )
        .await?;

        RadioThresholdIngestor::setup(
            &mut task_manager,
            pool.clone(),
            settings,
            file_upload.clone(),
            report_ingest,
            auth_client,
        )
        .await?;

        DataSessionIngestor::setup(&mut task_manager, pool.clone(), settings).await?;

        Rewarder::setup(
            &mut task_manager,
            pool,
            settings,
            file_upload,
            carrier_client,
            hex_boosting_client,
            speedtests_avg,
        )
        .await?;

        task_manager.start().await
    }
}
