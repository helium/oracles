use crate::{
    iceberg,
    speedtests_average::{SpeedtestAverage, SPEEDTEST_LAPSE},
    Settings,
};
use crate::{GatewayResolution, GatewayResolver};
use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source,
    file_upload::FileUpload, BucketClient,
};
use file_store_oracles::{
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SpeedtestAvg as SpeedtestAvgProto, SpeedtestAvgValidity, SpeedtestIngestReportV1,
    SpeedtestVerificationResult as SpeedtestResult, VerifiedSpeedtest as VerifiedSpeedtestProto,
};
use retainer::Cache;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub const BYTES_PER_MEGABIT: u64 = 125_000;

const SPEEDTEST_AVG_MAX_DATA_POINTS: usize = 6;
// The limit must be 300 megabits per second.
// Values in proto are in bytes/sec format.
// Convert 300 megabits per second to bytes per second.
const SPEEDTEST_MAX_BYTES_PER_SECOND: u64 = 300 * BYTES_PER_MEGABIT;

#[derive(Debug, Clone)]
pub struct Speedtest {
    pub report: CellSpeedtest,
}

// ── SpeedtestDaemon ───────────────────────────────────────────────────────────

pub struct SpeedtestDaemon<GIR> {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_info_resolver: GIR,
    recent_speedtests: RecentSpeedtests,
    speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
    speedtest_avg_file_sink: FileSinkClient<SpeedtestAvgProto>,
    verified_speedtest_file_sink: FileSinkClient<VerifiedSpeedtestProto>,
    iceberg_writer: iceberg::SpeedtestWriter,
    speedtest_avg_iceberg_writer: iceberg::SpeedtestAvgWriter,
}

impl<GIR> SpeedtestDaemon<GIR>
where
    GIR: GatewayResolver,
{
    #[expect(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        bucket_client: BucketClient,
        trino: trino_client::Client,
        gateway_resolver: GIR,
        iceberg_writer: iceberg::SpeedtestWriter,
        speedtest_avg_iceberg_writer: iceberg::SpeedtestAvgWriter,
    ) -> anyhow::Result<impl ManagedTask> {
        let (speedtests_avg, speedtests_avg_server) = SpeedtestAvgProto::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

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
            RecentSpeedtests::from_trino(&trino).await,
            gateway_resolver,
            speedtests,
            speedtests_avg,
            speedtests_validity,
            iceberg_writer,
            speedtest_avg_iceberg_writer,
        );

        Ok(TaskManager::builder()
            .add_task(speedtests_validity_server)
            .add_task(speedtests_avg_server)
            .add_task(speedtests_server)
            .add_task(speedtest_daemon)
            .build())
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        recent_speedtests: RecentSpeedtests,
        gateway_info_resolver: GIR,
        speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
        speedtest_avg_file_sink: FileSinkClient<SpeedtestAvgProto>,
        verified_speedtest_file_sink: FileSinkClient<VerifiedSpeedtestProto>,
        iceberg_writer: iceberg::SpeedtestWriter,
        speedtest_avg_iceberg_writer: iceberg::SpeedtestAvgWriter,
    ) -> Self {
        Self {
            pool,
            gateway_info_resolver,
            recent_speedtests,
            speedtests,
            speedtest_avg_file_sink,
            verified_speedtest_file_sink,
            iceberg_writer,
            speedtest_avg_iceberg_writer,
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
        let mut iceberg_avg_records = Vec::new();
        let mut invalid_iceberg_avg_records = Vec::new();

        while let Some(speedtest_report) = speedtests.next().await {
            let result = self.validate_speedtest(&speedtest_report).await?;
            if result == SpeedtestResult::SpeedtestValid {
                let latest_speedtests = self
                    .recent_speedtests
                    .push(Speedtest {
                        report: speedtest_report.report.clone(),
                    })
                    .await;
                let average = SpeedtestAverage::from(latest_speedtests);
                average.write(&self.speedtest_avg_file_sink).await?;

                iceberg_records.push(iceberg::IcebergSpeedtest::from(&speedtest_report));

                let avg_record = iceberg::IcebergSpeedtestAvg::from(&average);
                if average.validity == SpeedtestAvgValidity::Valid {
                    iceberg_avg_records.push(avg_record);
                } else {
                    invalid_iceberg_avg_records.push(iceberg::IcebergInvalidSpeedtestAvg::new(
                        avg_record,
                        average.validity,
                    ));
                }
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
        self.speedtest_avg_iceberg_writer
            .write(&write_id, iceberg_avg_records, invalid_iceberg_avg_records)
            .await?;

        self.speedtest_avg_file_sink.commit().await?;
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

/// The rolling window of a hotspot's most recent speedtests, held in memory.
///
/// Speedtests are no longer written to Postgres, so the running average that
/// accompanies each report can't be recovered with a per-report `SELECT`
/// anymore. This keeps the same window the old query produced — up to
/// `SPEEDTEST_AVG_MAX_DATA_POINTS` samples within [`SPEEDTEST_LAPSE`] hours —
/// per hotspot instead, warmed once at startup from `poc.speedtests` in Trino so
/// a restart doesn't reset every hotspot to a single-sample average.
///
/// Entries expire on their own after [`SPEEDTEST_LAPSE`] hours of inactivity, so
/// hotspots that stop reporting fall out without an explicit purge.
#[derive(Clone)]
pub struct RecentSpeedtests {
    windows: Arc<Cache<PublicKeyBinary, Vec<Speedtest>>>,
}

impl RecentSpeedtests {
    pub fn new() -> Self {
        let windows = Arc::new(Cache::new());
        let windows_clone = windows.clone();
        tokio::spawn(async move {
            windows_clone
                .monitor(4, 0.25, Duration::from_secs(60 * 60))
                .await
        });
        Self { windows }
    }

    /// An empty cache warmed with each hotspot's most recent speedtests.
    ///
    /// A warm-up failure is not fatal — windows refill from live reports; until
    /// they do, averages are computed over fewer samples than usual.
    pub async fn from_trino(trino: &trino_client::Client) -> Self {
        let cache = Self::new();
        match cache.warm(trino).await {
            Ok(loaded) => tracing::info!(loaded, "warmed speedtest windows from trino"),
            Err(err) => tracing::error!(
                ?err,
                "failed to warm speedtest windows; continuing with empty windows"
            ),
        }
        cache
    }

    async fn warm(&self, trino: &trino_client::Client) -> anyhow::Result<usize> {
        let since = Utc::now() - chrono::Duration::hours(SPEEDTEST_LAPSE);
        let rows =
            iceberg::speedtest::recent_speedtests(trino, since, SPEEDTEST_AVG_MAX_DATA_POINTS)
                .await?;

        let mut windows: HashMap<PublicKeyBinary, Vec<Speedtest>> = HashMap::new();
        for row in rows {
            let pubkey: PublicKeyBinary = match row.hotspot_pubkey.parse() {
                Ok(pubkey) => pubkey,
                Err(err) => {
                    tracing::warn!(
                        pubkey = row.hotspot_pubkey,
                        ?err,
                        "skipping unparsable hotspot key while warming speedtest windows"
                    );
                    continue;
                }
            };
            windows.entry(pubkey.clone()).or_default().push(Speedtest {
                report: CellSpeedtest {
                    pubkey,
                    serial: row.serial,
                    timestamp: row.timestamp.with_timezone(&Utc),
                    upload_speed: row.upload_speed,
                    download_speed: row.download_speed,
                    latency: row.latency,
                },
            });
        }

        let loaded = windows.len();
        for (pubkey, mut window) in windows {
            sort_and_cap(&mut window);
            self.store(pubkey, window).await;
        }
        Ok(loaded)
    }

    /// Record `speedtest` and return the window used to average it: the most
    /// recent samples at or before its own timestamp, within
    /// [`SPEEDTEST_LAPSE`] hours of it.
    ///
    /// Mirrors the old `get_latest_speedtests_for_pubkey` query, which ran after
    /// the insert and so always saw the report it was averaging.
    pub async fn push(&self, speedtest: Speedtest) -> Vec<Speedtest> {
        let pubkey = speedtest.report.pubkey.clone();
        let timestamp = speedtest.report.timestamp;

        let mut known = match self.windows.get(&pubkey).await {
            Some(existing) => existing.clone(),
            None => Vec::new(),
        };

        // The old table had a `(pubkey, timestamp)` unique constraint and
        // ignored conflicting inserts; do the same rather than double-count a
        // replayed report.
        if !known.iter().any(|s| s.report.timestamp == timestamp) {
            known.push(speedtest);
        }
        known.sort_by(|a, b| b.report.timestamp.cmp(&a.report.timestamp));

        // The average covers this report and the ones preceding it. Selecting
        // before capping matters for a report that arrives late: it can be older
        // than everything already cached, and would otherwise be trimmed out of
        // the very window meant to average it.
        let oldest = timestamp - chrono::Duration::hours(SPEEDTEST_LAPSE);
        let window: Vec<Speedtest> = known
            .iter()
            .filter(|s| s.report.timestamp >= oldest && s.report.timestamp <= timestamp)
            .take(SPEEDTEST_AVG_MAX_DATA_POINTS)
            .cloned()
            .collect();

        known.truncate(SPEEDTEST_AVG_MAX_DATA_POINTS);
        self.store(pubkey, known).await;

        window
    }

    async fn store(&self, pubkey: PublicKeyBinary, window: Vec<Speedtest>) {
        self.windows
            .insert(
                pubkey,
                window,
                Duration::from_secs(SPEEDTEST_LAPSE as u64 * 60 * 60),
            )
            .await;
    }
}

impl Default for RecentSpeedtests {
    fn default() -> Self {
        Self::new()
    }
}

/// Newest first, capped at the number of samples an average uses.
fn sort_and_cap(window: &mut Vec<Speedtest>) {
    window.sort_by(|a, b| b.report.timestamp.cmp(&a.report.timestamp));
    window.truncate(SPEEDTEST_AVG_MAX_DATA_POINTS);
}

#[cfg(test)]
mod recent_speedtests_tests {
    use super::*;
    use chrono::{DateTime, Duration as ChronoDuration};

    fn hotspot(n: u8) -> PublicKeyBinary {
        PublicKeyBinary::from(vec![n])
    }

    fn speedtest(pubkey: &PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey: pubkey.clone(),
                serial: "serial".to_string(),
                timestamp,
                upload_speed: 10 * BYTES_PER_MEGABIT,
                download_speed: 100 * BYTES_PER_MEGABIT,
                latency: 25,
            },
        }
    }

    #[tokio::test]
    async fn window_accumulates_newest_first() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        cache
            .push(speedtest(&pubkey, now - ChronoDuration::hours(2)))
            .await;
        let window = cache
            .push(speedtest(&pubkey, now - ChronoDuration::hours(1)))
            .await;

        assert_eq!(2, window.len());
        assert_eq!(now - ChronoDuration::hours(1), window[0].report.timestamp);
        assert_eq!(now - ChronoDuration::hours(2), window[1].report.timestamp);
    }

    #[tokio::test]
    async fn window_is_capped_at_the_average_sample_count() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        let mut window = Vec::new();
        for minutes in (1..=(SPEEDTEST_AVG_MAX_DATA_POINTS as i64 + 3)).rev() {
            window = cache
                .push(speedtest(&pubkey, now - ChronoDuration::minutes(minutes)))
                .await;
        }

        assert_eq!(SPEEDTEST_AVG_MAX_DATA_POINTS, window.len());
    }

    #[tokio::test]
    async fn replaying_a_report_does_not_double_count_it() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        cache.push(speedtest(&pubkey, now)).await;
        let window = cache.push(speedtest(&pubkey, now)).await;

        assert_eq!(1, window.len());
    }

    #[tokio::test]
    async fn window_excludes_samples_outside_the_lapse() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        cache
            .push(speedtest(
                &pubkey,
                now - ChronoDuration::hours(SPEEDTEST_LAPSE + 1),
            ))
            .await;
        let window = cache.push(speedtest(&pubkey, now)).await;

        assert_eq!(1, window.len(), "stale sample should not be averaged");
        assert_eq!(now, window[0].report.timestamp);
    }

    #[tokio::test]
    async fn window_excludes_samples_newer_than_the_report() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        cache.push(speedtest(&pubkey, now)).await;
        // A late-arriving report is averaged against what preceded *it*.
        let window = cache
            .push(speedtest(&pubkey, now - ChronoDuration::hours(1)))
            .await;

        assert_eq!(1, window.len());
        assert_eq!(now - ChronoDuration::hours(1), window[0].report.timestamp);
    }

    /// A report can arrive after newer ones have already been cached. It must
    /// still appear in its own average rather than being trimmed away by the
    /// window cap.
    #[tokio::test]
    async fn a_late_report_is_averaged_with_what_preceded_it() {
        let now = Utc::now();
        let pubkey = hotspot(1);
        let cache = RecentSpeedtests::new();

        // Fill the window with newer reports.
        for minutes in (1..=SPEEDTEST_AVG_MAX_DATA_POINTS as i64).rev() {
            cache
                .push(speedtest(&pubkey, now - ChronoDuration::minutes(minutes)))
                .await;
        }

        let late = now - ChronoDuration::hours(1);
        let window = cache.push(speedtest(&pubkey, late)).await;

        assert_eq!(1, window.len());
        assert_eq!(late, window[0].report.timestamp);
    }

    #[tokio::test]
    async fn windows_are_kept_per_hotspot() {
        let now = Utc::now();
        let cache = RecentSpeedtests::new();

        cache.push(speedtest(&hotspot(1), now)).await;
        let window = cache.push(speedtest(&hotspot(2), now)).await;

        assert_eq!(1, window.len());
        assert_eq!(hotspot(2), window[0].report.pubkey);
    }
}
