use super::{process_validated_heartbeats, Heartbeat, ValidatedHeartbeat};
use crate::coverage::{CoverageClaimTimeCache, CoveredHexCache};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CbrsHeartbeatIngestReport,
};
use futures::{stream::StreamExt, TryFutureExt};
use futures_util::future::BoxFuture;
use mobile_config::GatewayClient;
use retainer::Cache;

use std::{sync::Arc, time};
use tokio::sync::mpsc::Receiver;

pub struct HeartbeatDaemon {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_client: GatewayClient,
    heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
    modeled_coverage_start: DateTime<Utc>,
    heartbeat_sink: FileSinkClient,
    seniority_sink: FileSinkClient,
}

impl HeartbeatDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
        modeled_coverage_start: DateTime<Utc>,
        heartbeat_sink: FileSinkClient,
        seniority_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            heartbeats,
            modeled_coverage_start,
            heartbeat_sink,
            seniority_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            tracing::info!("Starting CBRS HeartbeatDaemon");
            let heartbeat_cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

            let heartbeat_cache_clone = heartbeat_cache.clone();
            tokio::spawn(async move {
                heartbeat_cache_clone
                    .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            let coverage_claim_time_cache = CoverageClaimTimeCache::new();
            let covered_hex_cache = CoveredHexCache::new(&self.pool);

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("CBRS HeartbeatDaemon shutting down");
                        break;
                    }
                    Some(file) = self.heartbeats.recv() => self.process_file(
                        file,
                        &heartbeat_cache,
                        &coverage_claim_time_cache,
                        &covered_hex_cache,
                    ).await?,
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file<'a>(
        &'a self,
        file: FileInfoStream<CbrsHeartbeatIngestReport>,
        heartbeat_cache: &Arc<Cache<(String, DateTime<Utc>), ()>>,
        coverage_claim_time_cache: &'a CoverageClaimTimeCache,
        covered_hex_cache: &'a CoveredHexCache,
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
                async move { hb_cache.get(&id).await.is_some() }
            });
        process_validated_heartbeats(
            ValidatedHeartbeat::validate_heartbeats(
                heartbeats,
                &self.gateway_client,
                covered_hex_cache,
                &epoch,
            ),
            heartbeat_cache,
            coverage_claim_time_cache,
            self.modeled_coverage_start,
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
