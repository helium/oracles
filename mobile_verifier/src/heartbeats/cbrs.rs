use super::{
    location_cache::LocationCache, process_validated_heartbeats, Heartbeat, ValidatedHeartbeat,
};
use crate::{
    coverage::{CoverageClaimTimeCache, CoverageObjectCache},
    geofence::GeofenceValidator,
    GatewayResolver, Settings,
};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    heartbeat::CbrsHeartbeatIngestReport,
    FileStore, FileType,
};
use futures::{stream::StreamExt, TryFutureExt};
use helium_proto::services::poc_mobile as proto;
use sqlx::{Pool, Postgres};
use std::{
    sync::Arc,
    time::{self, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct CbrsHeartbeatDaemon<GIR, GFV> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: GIR,
    heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
    max_distance_to_coverage: u32,
    heartbeat_sink: FileSinkClient<proto::Heartbeat>,
    seniority_sink: FileSinkClient<proto::SeniorityUpdate>,
    geofence: GFV,
    location_cache: LocationCache,
}

impl<GIR, GFV> CbrsHeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
    GFV: GeofenceValidator,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_store: FileStore,
        gateway_resolver: GIR,
        valid_heartbeats: FileSinkClient<proto::Heartbeat>,
        seniority_updates: FileSinkClient<proto::SeniorityUpdate>,
        geofence: GFV,
        location_cache: LocationCache,
    ) -> anyhow::Result<impl ManagedTask> {
        // CBRS Heartbeats
        let (cbrs_heartbeats, cbrs_heartbeats_server) =
            file_source::continuous_source::<CbrsHeartbeatIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::CbrsHeartbeatIngestReport.to_string())
                .queue_size(1)
                .create()
                .await?;

        let cbrs_heartbeat_daemon = CbrsHeartbeatDaemon::new(
            pool,
            gateway_resolver,
            cbrs_heartbeats,
            settings.max_distance_from_coverage,
            valid_heartbeats,
            seniority_updates,
            geofence,
            location_cache,
        );

        Ok(TaskManager::builder()
            .add_task(cbrs_heartbeats_server)
            .add_task(cbrs_heartbeat_daemon)
            .build())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_info_resolver: GIR,
        heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
        max_distance_to_coverage: u32,
        heartbeat_sink: FileSinkClient<proto::Heartbeat>,
        seniority_sink: FileSinkClient<proto::SeniorityUpdate>,
        geofence: GFV,
        location_cache: LocationCache,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            heartbeats,
            max_distance_to_coverage,
            heartbeat_sink,
            seniority_sink,
            geofence,
            location_cache,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting CBRS HeartbeatDaemon");
        let heartbeat_cache = Arc::new(retainer::Cache::<(String, DateTime<Utc>), ()>::new());

        let heartbeat_cache_clone = heartbeat_cache.clone();
        tokio::spawn(async move {
            heartbeat_cache_clone
                .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                .await
        });

        let coverage_claim_time_cache = CoverageClaimTimeCache::new();
        let coverage_object_cache = CoverageObjectCache::new(&self.pool);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("CBRS HeartbeatDaemon shutting down");
                    break;
                }
                Some(file) = self.heartbeats.recv() => {
                    let start = Instant::now();
                    self.process_file(
                        file,
                        &heartbeat_cache,
                        &coverage_claim_time_cache,
                        &coverage_object_cache,
                    ).await?;
                    metrics::histogram!("cbrs_heartbeat_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_file(
        &self,
        file: FileInfoStream<CbrsHeartbeatIngestReport>,
        heartbeat_cache: &Arc<retainer::Cache<(String, DateTime<Utc>), ()>>,
        coverage_claim_time_cache: &CoverageClaimTimeCache,
        coverage_object_cache: &CoverageObjectCache,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing CBRS heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let heartbeat_cache_clone = heartbeat_cache.clone();
        let heartbeats = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from)
            .filter(move |h| {
                let hb_cache = heartbeat_cache_clone.clone();
                let id = h.id().unwrap();
                async move { hb_cache.get(&id).await.is_none() }
            });
        process_validated_heartbeats(
            ValidatedHeartbeat::validate_heartbeats(
                heartbeats,
                &self.gateway_info_resolver,
                coverage_object_cache,
                &self.location_cache,
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

impl<GIR, GFV> ManagedTask for CbrsHeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
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
