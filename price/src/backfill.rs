use crate::{
    iceberg::{self, IcebergPriceReport, PriceWriter},
    settings::Settings,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source, FileInfo};
use file_store_oracles::FileType;
use futures::StreamExt;
use helium_proto::PriceReportV1;
use sqlx::{PgPool, Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Process name for tracking backfill progress in `files_processed`.
    #[clap(long, default_value = "backfill")]
    process_name: String,

    /// Start processing files after this timestamp.
    /// Format: RFC 3339 (e.g. 2024-01-01T00:00:00Z)
    #[clap(long)]
    start_after: DateTime<Utc>,

    /// Stop processing files when their timestamp is >= this value.
    /// Set this to the date Iceberg was first enabled in production so the
    /// backfiller does not overlap with the daemon's real-time writes.
    /// Format: RFC 3339 (e.g. 2026-04-24T00:00:00Z)
    #[clap(long)]
    stop_after: DateTime<Utc>,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let database = settings
            .database
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("database settings required for backfill"))?;
        let iceberg_settings = settings
            .iceberg_settings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("iceberg_settings required for backfill"))?;

        let pool = database.connect("price-backfill").await?;
        sqlx::migrate!().run(&pool).await?;

        let writer = iceberg::get_writer(iceberg_settings).await?;

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            "starting price backfill"
        );

        let task = PriceReportBackfiller::create_managed_task(
            pool,
            settings.output_bucket_client().await,
            writer,
            BackfillOptions {
                process_name: self.process_name,
                start_after: self.start_after,
                stop_after: self.stop_after,
            },
        )
        .await?;

        task.start().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub process_name: String,
    pub start_after: DateTime<Utc>,
    pub stop_after: DateTime<Utc>,
}

pub struct PriceReportBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<PriceReportV1>>,
    writer: PriceWriter,
    done: bool,
}

impl PriceReportBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<PriceReportV1>>,
        writer: PriceWriter,
    ) -> Self {
        Self {
            pool,
            reports,
            writer,
            done: false,
        }
    }

    pub async fn create(
        pool: PgPool,
        bucket_client: file_store::BucketClient,
        writer: PriceWriter,
        options: BackfillOptions,
    ) -> Result<(Self, impl ManagedTask)> {
        let (reports, reports_server) =
            file_source::Continuous::prost_source::<PriceReportV1, _, _>()
                .state(pool.clone())
                .bucket_client(bucket_client)
                .prefix(FileType::PriceReport.to_string())
                .lookback_start_after(options.start_after)
                .stop_after(options.stop_after)
                .process_name(options.process_name)
                .create()
                .await?;

        Ok((
            PriceReportBackfiller::new(pool, reports, writer),
            reports_server,
        ))
    }

    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: file_store::BucketClient,
        writer: PriceWriter,
        options: BackfillOptions,
    ) -> Result<TaskManager> {
        let (backfiller, server) = Self::create(pool, bucket_client, writer, options).await?;

        Ok(TaskManager::builder()
            .add_task(server)
            .add_task(backfiller)
            .build())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!("price backfiller starting");
        loop {
            if self.done {
                tracing::info!("price backfiller complete");
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("price backfiller shutting down");
                    return Ok(());
                }
                file = self.reports.recv() => {
                    self.handle(file).await?;
                }
            }
        }
    }

    async fn handle(&mut self, file: Option<FileInfoStream<PriceReportV1>>) -> Result<()> {
        let Some(file_info_stream) = file else {
            tracing::info!("price backfiller completed (channel closed)");
            self.done = true;
            return Ok(());
        };

        let file_info = file_info_stream.file_info.clone();
        tracing::info!(
            file = %file_info,
            timestamp = %file_info.timestamp,
            age = %format_file_age(&file_info),
            "backfilling price report file"
        );

        let mut txn = self.pool.begin().await?;
        let write_id = file_info.key.clone();
        let reports = file_info_stream.into_stream(&mut txn).await?;
        let all_reports: Vec<_> = reports.collect().await;
        let total = all_reports.len();

        let iceberg_records: Vec<_> = all_reports
            .iter()
            .filter_map(|report| match IcebergPriceReport::try_from(report) {
                Ok(record) => Some(record),
                Err(err) => {
                    tracing::warn!(?err, "skipping invalid price report");
                    None
                }
            })
            .collect();
        let valid = iceberg_records.len();
        let skipped = total - valid;

        self.writer
            .write_idempotent(&write_id, iceberg_records)
            .await
            .context("writing price reports to iceberg")?;

        txn.commit().await.context("committing db transaction")?;

        tracing::info!(
            file = %file_info,
            valid,
            skipped,
            "backfilled price report file"
        );
        Ok(())
    }
}

impl ManagedTask for PriceReportBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

fn format_file_age(file_info: &FileInfo) -> String {
    let age = Utc::now().signed_duration_since(file_info.timestamp);
    let total = age.num_seconds();
    if total < 0 {
        return "in the future".to_string();
    }
    let days = age.num_days();
    let hours = age.num_hours() % 24;
    let minutes = age.num_minutes() % 60;
    match (days, hours, minutes) {
        (0, 0, m) => format!("{m}m ago"),
        (0, h, m) => format!("{h}h {m}m ago"),
        (d, h, _) => format!("{d}d {h}h ago"),
    }
}
