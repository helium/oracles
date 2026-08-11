use std::time::Duration;

use crate::{
    banning::ingestor::BanIngestor, data_session::DataSessionIngestor,
    gateway::TrinoGatewayResolver, geofence::Geofence, heartbeats::wifi::WifiHeartbeatDaemon,
    iceberg, rewarder::Rewarder, speedtests::SpeedtestDaemon, telemetry, Settings,
};
use anyhow::Result;
use file_store::file_upload;
use file_store_oracles::traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt};
use helium_proto::services::poc_mobile::Heartbeat;
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
            file_upload::FileUpload::from_bucket_client(settings.buckets.output.connect().await)
                .await;

        let (valid_heartbeats, valid_heartbeats_server) = Heartbeat::file_sink(
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

        let ingest_bucket_client = settings.buckets.ingest.connect().await;

        let (poc_writers, reward_writers) =
            if let Some(ref iceberg_settings) = settings.iceberg_settings {
                (
                    iceberg::PocWriters::from_settings(iceberg_settings).await?,
                    Some(iceberg::get_reward_writers(iceberg_settings).await?),
                )
            } else {
                (iceberg::PocWriters::noop(), None)
            };

        // Trino query client for the reward pipeline: recovers the epoch's HNT
        // price from on-chain deployer-cap data, and reads/compares data-transfer
        // sessions against Postgres (see `data_session::DataSessionSource`).
        // Required — rewarding has no other price source. `from_settings` is
        // synchronous and starts the JWT-file watcher if configured. It also
        // backs the gateway resolver below (replacing mobile-config).
        let trino_client = trino_client::Client::from_settings(&settings.trino)?;

        // Gateway resolution and authorization now come from Trino + settings
        // instead of the mobile-config gRPC server.
        let gateway_resolver =
            TrinoGatewayResolver::new(trino_client.clone(), settings.gateway_refresh_interval)
                .await;
        let gateway_refresher = gateway_resolver.refresher();
        let authorized_keys = settings.authorized_keys()?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_heartbeats_server)
            .add_task(task_manager::periodic(gateway_refresher))
            .add_task(
                WifiHeartbeatDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    ingest_bucket_client.clone(),
                    gateway_resolver.clone(),
                    valid_heartbeats,
                    usa_and_mexico_geofence,
                    poc_writers.heartbeat,
                )
                .await?,
            )
            .add_task(
                SpeedtestDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    file_upload.clone(),
                    ingest_bucket_client.clone(),
                    gateway_resolver.clone(),
                    poc_writers.speedtest,
                    poc_writers.speedtest_avg,
                )
                .await?,
            )
            .add_task(
                DataSessionIngestor::create_managed_task(
                    pool.clone(),
                    settings,
                    settings.buckets.data_transfer.connect().await,
                )
                .await?,
            )
            .add_task(
                BanIngestor::create_managed_task(
                    pool.clone(),
                    file_upload.clone(),
                    ingest_bucket_client.clone(),
                    authorized_keys,
                    settings,
                    poc_writers.ban,
                )
                .await?,
            )
            .add_task(
                Rewarder::create_managed_task(
                    pool,
                    settings,
                    file_upload,
                    reward_writers,
                    trino_client,
                )
                .await?,
            )
            .build()
            .start()
            .await?;
        Ok(())
    }
}
