use crate::{iceberg, Settings};
use crate::{GatewayResolution, GatewayResolver};
use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source,
    file_upload::FileUpload, BucketClient,
};
use file_store_oracles::{
    speedtest::CellSpeedtestIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use futures::stream::StreamExt;
use helium_proto::services::poc_mobile::{
    SpeedtestIngestReportV1, SpeedtestVerificationResult as SpeedtestResult,
    VerifiedSpeedtest as VerifiedSpeedtestProto,
};
use sqlx::PgPool;
use std::time::{Duration, Instant};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub const BYTES_PER_MEGABIT: u64 = 125_000;

// The limit must be 300 megabits per second.
// Values in proto are in bytes/sec format.
// Convert 300 megabits per second to bytes per second.
const SPEEDTEST_MAX_BYTES_PER_SECOND: u64 = 300 * BYTES_PER_MEGABIT;

// ── SpeedtestDaemon ───────────────────────────────────────────────────────────

pub struct SpeedtestDaemon<GIR> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: GIR,
    speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
    verified_speedtest_file_sink: FileSinkClient<VerifiedSpeedtestProto>,
    iceberg_writer: iceberg::SpeedtestWriter,
}

impl<GIR> SpeedtestDaemon<GIR>
where
    GIR: GatewayResolver,
{
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        bucket_client: BucketClient,
        gateway_resolver: GIR,
        iceberg_writer: iceberg::SpeedtestWriter,
    ) -> anyhow::Result<impl ManagedTask> {
        let (speedtests_validity, speedtests_validity_server) = VerifiedSpeedtestProto::file_sink(
            settings.store_base_path(),
            file_upload,
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (speedtests, speedtests_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .lookback_start_after(settings.start_after)
            .prefix(FileType::CellSpeedtestIngestReport.to_string())
            .create()
            .await?;

        let speedtest_daemon = SpeedtestDaemon::new(
            pool.clone(),
            gateway_resolver,
            speedtests,
            speedtests_validity,
            iceberg_writer,
        );

        Ok(TaskManager::builder()
            .add_task(speedtests_validity_server)
            .add_task(speedtests_server)
            .add_task(speedtest_daemon)
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_info_resolver: GIR,
        speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
        verified_speedtest_file_sink: FileSinkClient<VerifiedSpeedtestProto>,
        iceberg_writer: iceberg::SpeedtestWriter,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            speedtests,
            verified_speedtest_file_sink,
            iceberg_writer,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("SpeedtestDaemon shutting down");
                    break;
                }
                Some(file) = self.speedtests.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
                    metrics::histogram!("speedtest_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    pub async fn process_file(
        &self,
        file: FileInfoStream<CellSpeedtestIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing speedtest file {}", file.file_info.key);
        let write_id = file.file_info.key.clone();
        // Speedtests themselves are no longer stored in Postgres; the
        // transaction exists only so the file-info poller can record this file
        // as processed.
        let mut transaction = self.pool.begin().await?;
        let mut speedtests = file.into_stream(&mut transaction).await?;

        let mut iceberg_records = Vec::new();
        let mut invalid_iceberg_records = Vec::new();

        while let Some(speedtest_report) = speedtests.next().await {
            let result = self.validate_speedtest(&speedtest_report).await?;
            if result == SpeedtestResult::SpeedtestValid {
                iceberg_records.push(iceberg::IcebergSpeedtest::from(&speedtest_report));
            } else {
                invalid_iceberg_records.push(iceberg::IcebergInvalidSpeedtest::new(
                    iceberg::IcebergSpeedtest::from(&speedtest_report),
                    result,
                ));
            }
            // write out paper trail of speedtest validity
            self.write_verified_speedtest(speedtest_report, result)
                .await?;
        }

        self.iceberg_writer
            .write(&write_id, iceberg_records, invalid_iceberg_records)
            .await?;

        self.verified_speedtest_file_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn validate_speedtest(
        &self,
        speedtest: &CellSpeedtestIngestReport,
    ) -> anyhow::Result<SpeedtestResult> {
        if speedtest.report.upload_speed > SPEEDTEST_MAX_BYTES_PER_SECOND
            || speedtest.report.download_speed > SPEEDTEST_MAX_BYTES_PER_SECOND
        {
            return Ok(SpeedtestResult::SpeedtestValueOutOfBounds);
        }

        match self
            .gateway_info_resolver
            .resolve_gateway(&speedtest.report.pubkey, &speedtest.received_timestamp)
            .await?
        {
            GatewayResolution::DataOnly => Ok(SpeedtestResult::SpeedtestInvalidDeviceType),
            GatewayResolution::GatewayNotFound => Ok(SpeedtestResult::SpeedtestGatewayNotFound),
            // Asserted or on-chain-but-unasserted, non-data-only: valid.
            GatewayResolution::AssertedLocation(_, _) | GatewayResolution::GatewayNotAsserted => {
                Ok(SpeedtestResult::SpeedtestValid)
            }
        }
    }

    pub async fn write_verified_speedtest(
        &self,
        speedtest_report: CellSpeedtestIngestReport,
        result: SpeedtestResult,
    ) -> anyhow::Result<()> {
        let ingest_report: SpeedtestIngestReportV1 = speedtest_report.into();
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let proto = VerifiedSpeedtestProto {
            report: Some(ingest_report),
            result: result as i32,
            timestamp,
        };
        self.verified_speedtest_file_sink
            .write(proto, &[("result", result.as_str_name())])
            .await?
            .await??;
        Ok(())
    }
}

impl<GIR> ManagedTask for SpeedtestDaemon<GIR>
where
    GIR: GatewayResolver,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
