use std::time::Duration;

use crate::{
    banning::ingestor::BanIngestor,
    gateway::TrinoGatewayResolver,
    geofence::Geofence,
    heartbeats::{last_location::LocationCache, wifi::WifiHeartbeatDaemon},
    iceberg,
    rewarder::{Rewarder, RewarderState},
    speedtests::SpeedtestDaemon,
    telemetry, Settings,
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

        let rewarder_state = RewarderState::from_settings(settings, pool.clone());
        telemetry::initialize(&rewarder_state).await?;
        // Seed the mirror at startup so it is current from the first tick rather
        // than only after the first epoch is rewarded. Fails loudly here — an
        // unwritable path should surface at boot, not at cutover.
        rewarder_state.mirror_to_file().await?;

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

        let iceberg_settings = &settings.iceberg_settings;
        let poc_writers = iceberg::PocWriters::from_settings(iceberg_settings).await?;
        let reward_writers = iceberg::get_reward_writers(iceberg_settings).await?;

        // Trino query client for the reward pipeline: recovers the epoch's HNT
        // price from on-chain deployer-cap data, and reads burned data-transfer
        // sessions for the reward pool (see `iceberg::burned_session`).
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

        // Heartbeats that carry no location validation of their own inherit the
        // hotspot's last validated location from here. Failing to load it would
        // publish a zero location-trust score for every such hotspot, so this is
        // a hard startup failure rather than a degraded start.
        let location_cache = LocationCache::from_trino(&trino_client).await?;
        let location_cache_refresher = location_cache.refresher(
            trino_client.clone(),
            settings.location_cache_refresh_interval,
        );

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_heartbeats_server)
            .add_task(task_manager::periodic(gateway_refresher))
            .add_task(task_manager::periodic(location_cache_refresher))
            .add_task(
                WifiHeartbeatDaemon::create_managed_task(
                    pool.clone(),
                    settings,
                    ingest_bucket_client.clone(),
                    location_cache,
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
                    trino_client.clone(),
                    gateway_resolver.clone(),
                    poc_writers.speedtest,
                    poc_writers.speedtest_avg,
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
                    rewarder_state,
                )
                .await?,
            )
            .build()
            .start()
            .await?;
        Ok(())
    }
}
