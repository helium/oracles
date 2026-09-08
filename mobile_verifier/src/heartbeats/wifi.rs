use super::{process_validated_heartbeats, Heartbeat, ValidatedHeartbeat};
use crate::{
    geofence::GeofenceValidator, heartbeats::LocationCache, iceberg, GatewayResolver, Settings,
};
use chrono::Duration;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source, BucketClient,
};
use file_store_oracles::{wifi_heartbeat::WifiHeartbeatIngestReport, FileType};
use futures::stream::StreamExt;
use helium_proto::services::poc_mobile as proto;
use sqlx::PgPool;
use std::time::Instant;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct WifiHeartbeatDaemon<GIR, GFV> {
    pool: PgPool,
    /// Shared with the refresher task that reloads it (see
    /// [`LocationCache::refresher`]).
    location_cache: LocationCache,
    gateway_info_resolver: GIR,
    heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
    heartbeat_sink: FileSinkClient<proto::Heartbeat>,
    geofence: GFV,
    iceberg_writer: iceberg::HeartbeatWriter,
}

impl<GIR, GFV> WifiHeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
    GFV: GeofenceValidator,
{
    #[expect(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        bucket_client: BucketClient,
        location_cache: LocationCache,
        gateway_resolver: GIR,
        valid_heartbeats: FileSinkClient<proto::Heartbeat>,
        geofence: GFV,
        iceberg_writer: iceberg::HeartbeatWriter,
    ) -> anyhow::Result<impl ManagedTask> {
        // Wifi Heartbeats
        let (wifi_heartbeats, wifi_heartbeats_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .lookback_start_after(settings.start_after)
            .prefix(FileType::WifiHeartbeatIngestReport.to_string())
            .create()
            .await?;

        let wifi_heartbeat_daemon = WifiHeartbeatDaemon::new(
            pool,
            location_cache,
            gateway_resolver,
            wifi_heartbeats,
            valid_heartbeats,
            geofence,
            iceberg_writer,
        );

        Ok(TaskManager::builder()
            .add_task(wifi_heartbeats_server)
            .add_task(wifi_heartbeat_daemon)
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        location_cache: LocationCache,
        gateway_info_resolver: GIR,
        heartbeats: Receiver<FileInfoStream<WifiHeartbeatIngestReport>>,
        heartbeat_sink: FileSinkClient<proto::Heartbeat>,
        geofence: GFV,
        iceberg_writer: iceberg::HeartbeatWriter,
    ) -> Self {
        Self {
            pool,
            location_cache,
            gateway_info_resolver,
            heartbeats,
            heartbeat_sink,
            geofence,
            iceberg_writer,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting Wifi HeartbeatDaemon");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("Wifi HeartbeatDaemon shutting down");
                    break;
                }
                Some(file) = self.heartbeats.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
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
    ) -> anyhow::Result<()> {
        tracing::info!(
            file_key = file.file_info.key,
            "Processing WIFI heartbeat file"
        );
        // Heartbeats themselves are no longer stored in Postgres; the
        // transaction exists only so the file-info poller can record this file
        // as processed atomically with the rest of the batch.
        let mut transaction = self.pool.begin().await?;
        let write_id = file.file_info.key.clone();
        let epoch = (file.file_info.timestamp - Duration::hours(3))
            ..(file.file_info.timestamp + Duration::minutes(30));
        let heartbeats = file
            .into_stream(&mut transaction)
            .await?
            .map(Heartbeat::from);
        process_validated_heartbeats(
            ValidatedHeartbeat::validate_heartbeats(
                heartbeats,
                &self.gateway_info_resolver,
                &self.location_cache,
                &epoch,
                &self.geofence,
            ),
            &self.heartbeat_sink,
            &self.iceberg_writer,
            &write_id,
        )
        .await?;
        self.heartbeat_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl<GIR, GFV> ManagedTask for WifiHeartbeatDaemon<GIR, GFV>
where
    GIR: GatewayResolver,
    GFV: GeofenceValidator,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
