use crate::iceberg::{
    self, burned_session::IcebergBurnedDataTransferSession, session::IcebergDataTransferSession,
    BurnedDataTransferWriter, DataTransferWriter,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source, BucketClient, FileInfo};
use file_store_oracles::{
    mobile_session::VerifiedDataTransferIngestReport, mobile_transfer::ValidDataTransferSession,
    FileType,
};
use futures::StreamExt;
use helium_proto::services::poc_mobile::verified_data_transfer_ingest_report_v1::ReportStatus;
use sqlx::{PgPool, Pool, Postgres};
use std::time::Duration;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::settings::Settings;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Process name for tracking sessions backfill (avoids conflict with daemon)
    #[clap(long, default_value = "backfill")]
    process_name: String,

    /// Start processing files after this timestamp.
    /// Format: RFC 3339 (e.g., 2024-01-01T00:00:00Z)
    #[clap(long)]
    start_after: DateTime<Utc>,

    /// Stop processing files when their timestamp is > this value.
    /// Use this to avoid reprocessing files that the daemon has already handled.
    /// Format: RFC 3339 (e.g., 2025-02-25T00:00:00Z)
    #[clap(long)]
    stop_after: DateTime<Utc>,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let iceberg_settings = settings
            .iceberg_settings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("iceberg_settings required for backfill"))?;

        let pool = settings
            .database
            .connect("mobile-packet-verifier-backfill")
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let (session_writer, burned_session_writer) =
            iceberg::get_writers(iceberg_settings).await?;

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            "starting all backfills"
        );

        let options = BackfillOptions::new(&self.process_name, self.start_after, self.stop_after);

        let data_sessions_task = DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            settings.output_bucket.connect().await,
            session_writer,
            options.clone(),
        )
        .await?;

        let burned_sessions_task = BurnedSessionsBackfiller::create_managed_task(
            pool,
            settings.output_bucket.connect().await,
            burned_session_writer,
            options,
        )
        .await?;

        TaskManager::builder()
            .add_task(data_sessions_task)
            .add_task(burned_sessions_task)
            .build()
            .start()
            .await?;
        Ok(())
    }
}

/// Options for running sessions backfill.
#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub process_name: String,
    pub start_after: DateTime<Utc>,
    pub stop_after: DateTime<Utc>,
    pub poll_duration: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

impl BackfillOptions {
    pub fn new(
        process_name: impl Into<String>,
        start_after: DateTime<Utc>,
        stop_after: DateTime<Utc>,
    ) -> Self {
        Self {
            process_name: process_name.into(),
            start_after,
            stop_after,
            poll_duration: None,
            idle_timeout: None,
        }
    }

    pub fn poll_duration(mut self, poll_duration: Duration) -> Self {
        self.poll_duration = Some(poll_duration);
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }
}

pub struct DataSessionsBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<VerifiedDataTransferIngestReport>>,
    writer: DataTransferWriter,
}

impl DataSessionsBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<VerifiedDataTransferIngestReport>>,
        writer: DataTransferWriter,
    ) -> Self {
        Self {
            pool,
            reports,
            writer,
        }
    }

    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: DataTransferWriter,
        options: BackfillOptions,
    ) -> anyhow::Result<TaskManager> {
        let builder = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .prefix(FileType::VerifiedDataTransferSession.to_string())
            .lookback_start_after(options.start_after)
            .stop_after(options.stop_after)
            .process_name(options.process_name)
            .poll_duration_opt(options.poll_duration)
            .idle_timeout_opt(options.idle_timeout);

        let (reports, reports_server) = builder.create().await?;
        let backfiller = DataSessionsBackfiller::new(pool, reports, writer);

        Ok(TaskManager::builder()
            .add_task(reports_server)
            .add_task(backfiller)
            .build())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!("sessions backfiller starting");
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("sessions backfiller shutting down");
                    return Ok(());
                }
                file = self.reports.recv() => {
                    let Some(file_info_stream) = file else {
                        // Channel closed - poller reached stop_after or finished
                        tracing::info!("sessions backfiller completed");
                        return Ok(());
                    };
                    let age = format_file_age(&file_info_stream.file_info);
                    tracing::info!(
                        file = %file_info_stream.file_info,
                        timestamp = %file_info_stream.file_info.timestamp,
                        age = %age,
                        "received file"
                    );
                    self.handle_file(file_info_stream).await?;
                }
            }
        }
    }

    async fn handle_file(
        &self,
        file: FileInfoStream<VerifiedDataTransferIngestReport>,
    ) -> Result<()> {
        let file_info = file.file_info.clone();
        let age = format_file_age(&file_info);
        tracing::debug!(
            file = %file_info,
            timestamp = %file_info.timestamp,
            age = %age,
            "backfilling verified sessions file to iceberg"
        );

        let mut txn = self.pool.begin().await?;
        let mut iceberg_txn = self
            .writer
            .begin(file.file_info.as_ref())
            .await
            .context("beginning iceberg transaction")?;

        let reports = file.into_stream(&mut txn).await?;

        let all_reports: Vec<_> = reports.collect().await;
        let total = all_reports.len();

        let iceberg_sessions: Vec<_> = all_reports
            .into_iter()
            .filter(|verified_report| verified_report.status == ReportStatus::Valid)
            .map(|verified_report| IcebergDataTransferSession::from(verified_report.report))
            .collect();

        let valid_count = iceberg_sessions.len();
        let filtered_count = total - valid_count;
        iceberg_txn
            .write(iceberg_sessions)
            .await
            .context("writing to iceberg")?;

        iceberg_txn
            .publish()
            .await
            .context("publishing to iceberg")?;
        txn.commit().await.context("committing db transaction")?;

        tracing::info!(
            file = %file_info,
            valid_count,
            filtered_count,
            "backfilled verified sessions file"
        );
        Ok(())
    }
}

impl ManagedTask for DataSessionsBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

pub struct BurnedSessionsBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
    writer: BurnedDataTransferWriter,
}

impl BurnedSessionsBackfiller {
    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: BurnedDataTransferWriter,
        options: BackfillOptions,
    ) -> anyhow::Result<TaskManager> {
        let builder = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(options.start_after)
            .stop_after(options.stop_after)
            .process_name(options.process_name)
            .poll_duration_opt(options.poll_duration)
            .idle_timeout_opt(options.idle_timeout);

        let (burned_reports, burned_reports_server) = builder.create().await?;
        let burned_backfiller = BurnedSessionsBackfiller::new(pool, burned_reports, writer);

        Ok(TaskManager::builder()
            .add_task(burned_reports_server)
            .add_task(burned_backfiller)
            .build())
    }

    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
        writer: BurnedDataTransferWriter,
    ) -> Self {
        Self {
            pool,
            reports,
            writer,
        }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!("burned backfiller starting");
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("burned backfiller shutting down");
                    return Ok(());
                }
                file = self.reports.recv() => {
                    let Some(file_info_stream) = file else {
                        // Channel closed - poller reached stop_after or finished
                        tracing::info!("burned backfiller completed");
                        return Ok(());
                    };
                    let age = format_file_age(&file_info_stream.file_info);
                    tracing::info!(
                        file = %file_info_stream.file_info,
                        timestamp = %file_info_stream.file_info.timestamp,
                        age = %age,
                        "received file"
                    );
                    self.handle_file(file_info_stream).await?;
                }
            }
        }
    }

    async fn handle_file(&self, file: FileInfoStream<ValidDataTransferSession>) -> Result<()> {
        let file_info = file.file_info.clone();
        let age = format_file_age(&file_info);
        tracing::info!(
            file = %file_info,
            timestamp = %file_info.timestamp,
            age = %age,
            "backfilling burned sessions file to iceberg"
        );

        let mut txn = self.pool.begin().await?;
        let mut iceberg_txn = self
            .writer
            .begin(file.file_info.as_ref())
            .await
            .context("beginning iceberg transaction")?;

        let reports = file.into_stream(&mut txn).await?;

        let iceberg_sessions = reports
            .map(IcebergBurnedDataTransferSession::from)
            .collect::<Vec<_>>()
            .await;

        let count = iceberg_sessions.len();
        iceberg_txn
            .write(iceberg_sessions)
            .await
            .context("writing to iceberg")?;

        iceberg_txn
            .publish()
            .await
            .context("publishing to iceberg")?;
        txn.commit().await.context("committing db transaction")?;

        tracing::info!(file = %file_info, count, "backfilled burned sessions file");
        Ok(())
    }
}

impl ManagedTask for BurnedSessionsBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

/// Format the age of a file in a human-readable way (e.g., "2d 5h 30m ago")
fn format_file_age(file_info: &FileInfo) -> String {
    let age = Utc::now().signed_duration_since(file_info.timestamp);

    let total_seconds = age.num_seconds();
    if total_seconds < 0 {
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
