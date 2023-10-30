use super::{process_heartbeat_stream, Heartbeat};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CbrsHeartbeatIngestReport,
};
use futures::{stream::StreamExt, TryFutureExt};
use mobile_config::client::gateway_client::GatewayInfoResolver;
use retainer::Cache;

use std::{sync::Arc, time};
use tokio::sync::mpsc::Receiver;

pub struct HeartbeatDaemon<GIR> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: GIR,
    heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
    file_sink: FileSinkClient,
}

impl<GIR> HeartbeatDaemon<GIR>
where
    GIR: GatewayInfoResolver,
{
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_info_resolver: GIR,
        heartbeats: Receiver<FileInfoStream<CbrsHeartbeatIngestReport>>,
        file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            heartbeats,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            tracing::info!("Starting CBRS HeartbeatDaemon");
            let cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

            let cache_clone = cache.clone();
            tokio::spawn(async move {
                cache_clone
                    .monitor(4, 0.25, time::Duration::from_secs(60 * 60 * 3))
                    .await
            });

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("CBRS HeartbeatDaemon shutting down");
                        break;
                    }
                    Some(file) = self.heartbeats.recv() => self.process_file(file, &cache).await?,
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file(
        &self,
        file: FileInfoStream<CbrsHeartbeatIngestReport>,
        cache: &Cache<(String, DateTime<Utc>), ()>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing CBRS heartbeat file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        // map the ingest reports to our generic heartbeat  type
        let reports = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from);
        process_heartbeat_stream(
            reports,
            &self.gateway_info_resolver,
            &self.file_sink,
            cache,
            transaction,
            &epoch,
        )
        .await
    }
}
