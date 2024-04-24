use crate::{
    boosting_oracles::{
        footfall::Footfall, landtype::Landtype, urbanization::Urbanization,
        DataSetDownloaderDaemon, HexBoostData,
    },
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
use hextree::disktree::DiskTreeMap;
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

        let (oracle_boosting_reports, oracle_boosting_reports_server) =
            file_sink::FileSinkBuilder::new(
                FileType::OracleBoostingReport,
                store_base_path,
                file_upload.clone(),
                concat!(env!("CARGO_PKG_NAME"), "_oracle_boosting_report"),
            )
            .auto_commit(false)
            .roll_time(Duration::minutes(15))
            .create()
            .await?;

        let urbanization: Urbanization<DiskTreeMap, _> = Urbanization::new(usa_geofence.clone());
        let footfall: Footfall<DiskTreeMap> = Footfall::new();
        let landtype: Landtype<DiskTreeMap> = Landtype::new();
        let hex_boost_data = HexBoostData::new(footfall, landtype, urbanization);

        // Data sets and downloaders
        let data_sets_file_store = FileStore::from_settings(&settings.data_sets).await?;
        let footfall_data_set_downloader = DataSetDownloaderDaemon::new(
            pool.clone(),
            hex_boost_data.footfall.clone(),
            hex_boost_data.clone(),
            data_sets_file_store.clone(),
            oracle_boosting_reports.clone(),
            settings.data_sets_directory.clone(),
        )
        .await?;

        let landtype_data_set_downloader = DataSetDownloaderDaemon::new(
            pool.clone(),
            hex_boost_data.landtype.clone(),
            hex_boost_data.clone(),
            data_sets_file_store.clone(),
            oracle_boosting_reports.clone(),
            settings.data_sets_directory.clone(),
        )
        .await?;

        let urbanization_data_set_downloader = DataSetDownloaderDaemon::new(
            pool.clone(),
            hex_boost_data.urbanization.clone(),
            hex_boost_data.clone(),
            data_sets_file_store.clone(),
            oracle_boosting_reports,
            settings.data_sets_directory.clone(),
        )
        .await?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_heartbeats_server)
            .add_task(seniority_updates_server)
            .add_task(oracle_boosting_reports_server)
            .add_task(landtype_data_set_downloader)
            .add_task(footfall_data_set_downloader)
            .add_task(urbanization_data_set_downloader)
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
                    seniority_updates,
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
                    gateway_client,
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
                    hex_boost_data,
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
                    entity_client,
                )
                .await?,
            )
            .add_task(
                RadioThresholdIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    report_ingest,
                    auth_client,
                )
                .await?,
            )
            .add_task(DataSessionIngestor::create_managed_task(pool.clone(), settings).await?)
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
