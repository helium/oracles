use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source, BucketClient, FileInfo};
use file_store_oracles::{
    mobile_session::VerifiedDataTransferIngestReport, mobile_transfer::ValidDataTransferSession,
    FileType,
};
use futures::StreamExt;
use helium_iceberg::BatchedWriter;
use helium_proto::services::poc_mobile::verified_data_transfer_ingest_report_v1::ReportStatus;
use sqlx::{PgPool, Pool, Postgres};
use std::ops::ControlFlow;
use std::time::{Duration, Instant};
use task_manager::{ChannelConsumer, ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{iceberg, settings::Settings};

type BatchedDataTransferWriter = BatchedWriter<iceberg::session::IcebergDataTransferSession>;
type BatchedBurnedDataTranfserWriter =
    BatchedWriter<iceberg::burned_session::IcebergBurnedDataTransferSession>;

/// A server task returned by [`DataSessionsBackfiller::create`] and
/// [`BurnedSessionsBackfiller::create`].
///
/// Always implements [`ManagedTask`] so callers can unconditionally add it to a
/// [`TaskManager`] without any `Option`-handling. When no writer or options are
/// configured the inner server is `None` and `start_task` returns immediately.
pub struct BackfillPollerServer(Option<Box<dyn ManagedTask>>);

impl BackfillPollerServer {
    fn noop() -> Self {
        Self(None)
    }

    fn active(server: impl ManagedTask + 'static) -> Self {
        Self(Some(Box::new(server)))
    }
}

impl ManagedTask for BackfillPollerServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        match (*self).0 {
            Some(task) => task.start_task(shutdown),
            None => task_manager::spawn(async { anyhow::Ok(()) }),
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct BackfillSessionsCmd {
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

    /// Number of sessions to process in each batch.
    #[clap(long, default_value = "1000000")]
    batch_size: usize,
}

#[derive(Debug, clap::Args)]
pub struct BackfillBurnedCmd {
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

    /// Number of sessions to process in each batch.
    #[clap(long, default_value = "1000000")]
    batch_size: usize,
}

impl BackfillSessionsCmd {
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

        let catalog = iceberg_settings.connect().await?;
        let table_def = iceberg::session::table_definition()?;
        catalog
            .create_namespace_if_not_exists(table_def.namespace())
            .await?;
        let table = catalog.create_table_if_not_exists(table_def).await?;

        let (writer, writer_task) = helium_iceberg::BatchedWriter::new(
            table,
            helium_iceberg::BatchedWriterConfig::new(
                settings.cache.join("iceberg-spool/data-sessions"),
            )
            .with_max_batch_size(self.batch_size)
            .with_batch_timeout(Duration::from_mins(5)),
        );

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            batch_size = self.batch_size,
            "starting all backfills"
        );

        let options = BackfillOptions::new(&self.process_name, self.start_after, self.stop_after);

        let data_sessions_task = DataSessionsBackfiller::create_managed_task(
            pool.clone(),
            settings.output_bucket.connect().await,
            Some(writer),
            options.clone(),
        )
        .await?;

        TaskManager::builder()
            .add_task(writer_task)
            .add_task(data_sessions_task)
            .build()
            .start()
            .await?;
        Ok(())
    }
}

impl BackfillBurnedCmd {
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

        let catalog = iceberg_settings.connect().await?;
        let table_def = iceberg::burned_session::table_definition()?;
        catalog
            .create_namespace_if_not_exists(table_def.namespace())
            .await?;
        let table = catalog.create_table_if_not_exists(table_def).await?;

        let (writer, writer_task) = helium_iceberg::BatchedWriter::new(
            table,
            helium_iceberg::BatchedWriterConfig::new(
                settings.cache.join("iceberg-spool/burned-sessions"),
            )
            .with_max_batch_size(self.batch_size)
            .with_batch_timeout(Duration::from_mins(5)),
        );

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            batch_size = self.batch_size,
            "starting all backfills"
        );

        let options = BackfillOptions::new(&self.process_name, self.start_after, self.stop_after);

        let burned_sessions_task = BurnedSessionsBackfiller::create_managed_task(
            pool,
            settings.output_bucket.connect().await,
            Some(writer),
            options,
        )
        .await?;

        TaskManager::builder()
            .add_task(writer_task)
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
    writer: Option<BatchedDataTransferWriter>,
    done: bool,
}

impl DataSessionsBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<VerifiedDataTransferIngestReport>>,
        writer: Option<BatchedDataTransferWriter>,
    ) -> Self {
        let done = writer.is_none();
        Self {
            pool,
            reports,
            writer,
            done,
        }
    }

    pub async fn create(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: Option<BatchedDataTransferWriter>,
        options: Option<BackfillOptions>,
    ) -> anyhow::Result<(Self, BackfillPollerServer)> {
        let (Some(writer), Some(options)) = (writer, options) else {
            let (_, rx) = tokio::sync::mpsc::channel(1);
            return Ok((
                DataSessionsBackfiller::new(pool, rx, None),
                BackfillPollerServer::noop(),
            ));
        };

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
        let backfiller = DataSessionsBackfiller::new(pool, reports, Some(writer));

        Ok((backfiller, BackfillPollerServer::active(reports_server)))
    }

    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: Option<BatchedDataTransferWriter>,
        options: BackfillOptions,
    ) -> anyhow::Result<TaskManager> {
        let (backfiller, server) = Self::create(pool, bucket_client, writer, Some(options)).await?;

        Ok(TaskManager::builder()
            .add_task(server)
            .add_task(task_manager::channel_consumer(backfiller))
            .build())
    }

    async fn handle_file(
        &self,
        file: FileInfoStream<VerifiedDataTransferIngestReport>,
    ) -> Result<()> {
        let Some(ref writer) = self.writer else {
            return Ok(());
        };

        let start = Instant::now();

        let file_info = file.file_info.clone();
        let age = format_file_age(&file_info);
        tracing::debug!(
            file = %file_info,
            timestamp = %file_info.timestamp,
            age = %age,
            "backfilling verified sessions file to iceberg"
        );

        let mut txn = self.pool.begin().await?;

        let all: Vec<_> = file.into_stream(&mut txn).await?.collect().await;
        let total = all.len();

        let rows: Vec<_> = all
            .into_iter()
            .filter(|verified_report| verified_report.status == ReportStatus::Valid)
            .map(|verified_report| {
                iceberg::IcebergDataTransferSession::from(verified_report.report)
            })
            .collect();
        let written = rows.len();

        writer
            .queue_all(rows)
            .await
            .context("queueing data sessions backfill")?;

        txn.commit().await.context("committing db transaction")?;

        tracing::info!(
            file = %file_info,
            written,
            skipped = total - written,
            age = %age,
            duration = ?start.elapsed(),
            "backfilled verified sessions file"
        );
        Ok(())
    }
}

impl ChannelConsumer for DataSessionsBackfiller {
    type Item = FileInfoStream<VerifiedDataTransferIngestReport>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.reports.recv().await
    }

    async fn on_start(&mut self) -> anyhow::Result<ControlFlow<()>> {
        tracing::info!("sessions backfiller starting");
        if self.done {
            tracing::info!("sessions backfiller complete");
            Ok(ControlFlow::Break(()))
        } else {
            Ok(ControlFlow::Continue(()))
        }
    }

    async fn handle(&mut self, file_info_stream: Self::Item) -> anyhow::Result<()> {
        let age = format_file_age(&file_info_stream.file_info);
        tracing::info!(
            file = %file_info_stream.file_info,
            timestamp = %file_info_stream.file_info.timestamp,
            age = %age,
            "received file"
        );
        self.handle_file(file_info_stream).await
    }

    async fn on_receiver_closed(&mut self) -> anyhow::Result<ControlFlow<()>> {
        tracing::info!("session backfiller completed");
        self.done = true;
        Ok(ControlFlow::Break(()))
    }
}

pub struct BurnedSessionsBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
    writer: Option<BatchedBurnedDataTranfserWriter>,
    done: bool,
}

impl BurnedSessionsBackfiller {
    pub async fn create(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: Option<BatchedBurnedDataTranfserWriter>,
        options: Option<BackfillOptions>,
    ) -> anyhow::Result<(Self, BackfillPollerServer)> {
        let (Some(writer), Some(options)) = (writer, options) else {
            let (_, rx) = tokio::sync::mpsc::channel(1);
            return Ok((
                BurnedSessionsBackfiller::new(pool, rx, None),
                BackfillPollerServer::noop(),
            ));
        };

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
        let burned_backfiller = BurnedSessionsBackfiller::new(pool, burned_reports, Some(writer));

        Ok((
            burned_backfiller,
            BackfillPollerServer::active(burned_reports_server),
        ))
    }

    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: Option<BatchedBurnedDataTranfserWriter>,
        options: BackfillOptions,
    ) -> anyhow::Result<TaskManager> {
        let (burned_backfiller, server) =
            Self::create(pool, bucket_client, writer, Some(options)).await?;

        Ok(TaskManager::builder()
            .add_task(server)
            .add_task(task_manager::channel_consumer(burned_backfiller))
            .build())
    }

    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
        writer: Option<BatchedBurnedDataTranfserWriter>,
    ) -> Self {
        let done = writer.is_none();
        Self {
            pool,
            reports,
            writer,
            done,
        }
    }

    async fn handle_file(&self, file: FileInfoStream<ValidDataTransferSession>) -> Result<()> {
        let Some(ref writer) = self.writer else {
            return Ok(());
        };

        let start = Instant::now();

        let file_info = file.file_info.clone();
        let age = format_file_age(&file_info);
        tracing::info!(
            file = %file_info,
            timestamp = %file_info.timestamp,
            age = %age,
            "backfilling burned sessions file to iceberg"
        );

        let mut txn = self.pool.begin().await?;

        let all: Vec<_> = file.into_stream(&mut txn).await?.collect().await;

        let rows: Vec<_> = all
            .into_iter()
            .map(|session| {
                iceberg::IcebergBurnedDataTransferSession::from_session(session, &file_info)
            })
            .collect();
        let written = rows.len();

        writer
            .queue_all(rows)
            .await
            .context("writing burned sessions to iceberg")?;

        txn.commit().await.context("committing db transaction")?;

        tracing::info!(
            file = %file_info,
            written,
            age = %age,
            duration = ?start.elapsed(),
            "backfilled burned sessions file"
        );
        Ok(())
    }
}

impl ChannelConsumer for BurnedSessionsBackfiller {
    type Item = FileInfoStream<ValidDataTransferSession>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.reports.recv().await
    }

    async fn on_start(&mut self) -> anyhow::Result<ControlFlow<()>> {
        tracing::info!("burned backfiller starting");
        if self.done {
            tracing::info!("burned backfiller complete");
            Ok(ControlFlow::Break(()))
        } else {
            Ok(ControlFlow::Continue(()))
        }
    }

    async fn handle(&mut self, file_info_stream: Self::Item) -> anyhow::Result<()> {
        let age = format_file_age(&file_info_stream.file_info);
        tracing::info!(
            file = %file_info_stream.file_info,
            timestamp = %file_info_stream.file_info.timestamp,
            age = %age,
            "received file"
        );
        self.handle_file(file_info_stream).await
    }

    async fn on_receiver_closed(&mut self) -> anyhow::Result<ControlFlow<()>> {
        tracing::info!("burned backfiller completed");
        self.done = true;
        Ok(ControlFlow::Break(()))
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
