use super::{process_validated_heartbeats, Heartbeat, ValidatedHeartbeat};
use crate::{
    coverage::{CoverageClaimTimeCache, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::LocationCache,
    GatewayResolver, Settings,
};
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    wifi_heartbeat::WifiHeartbeatIngestReport,
    FileStore, FileType,
};
use futures::{stream::StreamExt, TryFutureExt};
use helium_proto::services::poc_mobile as proto;
use retainer::Cache;
use sqlx::{Pool, Postgres};
use std::{
    sync::Arc,
    time::{self, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct WifiHeartbeatDaemon<GFV> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: Arc<dyn GatewayResolver>,
    heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
    max_distance_to_coverage: u32,
    heartbeat_sink: FileSinkClient<proto::Heartbeat>,
    seniority_sink: FileSinkClient<proto::SeniorityUpdate>,
    geofence: GFV,
}

impl<GFV> WifiHeartbeatDaemon<GFV>
where
    GFV: GeofenceValidator,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_store: FileStore,
        gateway_resolver: Arc<dyn GatewayResolver>,
        valid_heartbeats: FileSinkClient<proto::Heartbeat>,
        seniority_updates: FileSinkClient<proto::SeniorityUpdate>,
        geofence: GFV,
    ) -> anyhow::Result<impl ManagedTask> {
        // Wifi Heartbeats
        let (wifi_heartbeats, wifi_heartbeats_server) =
            file_source::continuous_source::<WifiHeartbeatIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::WifiHeartbeatIngestReport.to_string())
                .create()
                .await?;

        let wifi_heartbeat_daemon = WifiHeartbeatDaemon::new(
            pool,
            gateway_resolver,
            wifi_heartbeats,
            settings.max_distance_from_coverage,
            valid_heartbeats,
            seniority_updates,
            geofence,
        );

        Ok(TaskManager::builder()
            .add_task(wifi_heartbeats_server)
            .add_task(wifi_heartbeat_daemon)
            .build())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_info_resolver: Arc<dyn GatewayResolver>,
        heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
        max_distance_to_coverage: u32,
        heartbeat_sink: FileSinkClient<proto::Heartbeat>,
        seniority_sink: FileSinkClient<proto::SeniorityUpdate>,
        geofence: GFV,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            heartbeats,
            max_distance_to_coverage,
            heartbeat_sink,
            seniority_sink,
            geofence,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting Wifi HeartbeatDaemon");
        let heartbeat_cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

        let heartbeat_cache_clone = heartbeat_cache.clone();
        tokio::spawn(async move {
            heartbeat_cache_clone
                .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                .await
        });

        let coverage_claim_time_cache = CoverageClaimTimeCache::new();
        let coverage_object_cache = CoverageObjectCache::new(&self.pool);
        let location_cache = LocationCache::new(&self.pool);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("Wifi HeartbeatDaemon shutting down");
                    break;
                }
                Some(file) = self.heartbeats.recv() => {
                    let start = Instant::now();
                    self.process_file(
                        file,
                        &heartbeat_cache,
                        &coverage_claim_time_cache,
                        &coverage_object_cache,
                        &location_cache
                    ).await?;
                    metrics::histogram!("wifi_heartbeat_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_file(
        &self,
        file: FileInfoStream<WifiHeartbeatIngestReport>,
        heartbeat_cache: &Cache<(String, DateTime<Utc>), ()>,
        coverage_claim_time_cache: &CoverageClaimTimeCache,
        coverage_object_cache: &CoverageObjectCache,
        location_cache: &LocationCache,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing WIFI heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let heartbeats = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from);
        process_validated_heartbeats(
            ValidatedHeartbeat::validate_heartbeats(
                heartbeats,
                self.gateway_info_resolver.clone(),
                coverage_object_cache,
                location_cache,
                self.max_distance_to_coverage,
                &epoch,
                &self.geofence,
            ),
            heartbeat_cache,
            coverage_claim_time_cache,
            &self.heartbeat_sink,
            &self.seniority_sink,
            &mut transaction,
        )
        .await?;
        self.heartbeat_sink.commit().await?;
        self.seniority_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl<GFV> ManagedTask for WifiHeartbeatDaemon<GFV>
where
    GFV: GeofenceValidator,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}
