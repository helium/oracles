use crate::iceberg::{
    self, burned_session::IcebergBurnedDataTransferSession, session::IcebergDataTransferSession,
    BurnedDataTransferWriter, DataTransferWriter,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source, BucketClient};
use file_store_oracles::{
    mobile_session::DataTransferSessionIngestReport, mobile_transfer::ValidDataTransferSession,
    FileType,
};
use futures::StreamExt;
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
            stop_after = %self.stop_after,
            "starting all backfills"
        );

        let options = BackfillOptions::new(&self.process_name, settings.start_after)
            .stop_after(self.stop_after);

        let sessions_task = SessionsBackfiller::create_managed_task(
            pool.clone(),
            settings.ingest_bucket.connect().await,
            session_writer,
            options.clone(),
        )
        .await?;

        let burned_session_task = BurnedBackfiller::create_managed_task(
            pool,
            settings.output_bucket.connect().await,
            burned_session_writer,
            options,
        )
        .await?;

        TaskManager::builder()
            .add_task(sessions_task)
            .add_task(burned_session_task)
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
    pub stop_after: Option<DateTime<Utc>>,
    pub poll_duration: Duration,
    pub idle_timeout: Duration,
}

impl BackfillOptions {
    pub fn new(process_name: impl Into<String>, start_after: DateTime<Utc>) -> Self {
        Self {
            process_name: process_name.into(),
            start_after,
            stop_after: None,
            poll_duration: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
        }
    }

    pub fn stop_after(mut self, stop_after: DateTime<Utc>) -> Self {
        self.stop_after = Some(stop_after);
        self
    }

    pub fn poll_duration(mut self, poll_duration: Duration) -> Self {
        self.poll_duration = poll_duration;
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

pub struct SessionsBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    writer: DataTransferWriter,
}

impl SessionsBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
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
    ) -> anyhow::Result<impl ManagedTask> {
        let mut builder = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(options.start_after)
            .process_name(options.process_name)
            .poll_duration(options.poll_duration)
            .idle_timeout(options.idle_timeout);

        if let Some(stop_after) = options.stop_after {
            builder = builder.stop_after(stop_after);
        }

        let (reports, reports_server) = builder.create().await?;
        let backfiller = SessionsBackfiller::new(pool, reports, writer);

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
                    tracing::info!(file = %file_info_stream.file_info, "received file");
                    self.handle_file(file_info_stream).await?;
                }
            }
        }
    }

    async fn handle_file(
        &self,
        file: FileInfoStream<DataTransferSessionIngestReport>,
    ) -> Result<()> {
        tracing::info!(file = %file.file_info, "backfilling sessions file to iceberg");

        let mut txn = self.pool.begin().await?;
        let mut iceberg_txn = self
            .writer
            .begin(file.file_info.as_ref())
            .await
            .context("beginning iceberg transaction")?;

        let reports = file.into_stream(&mut txn).await?;

        let iceberg_sessions = reports
            .map(IcebergDataTransferSession::from)
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

        tracing::info!(count, "backfilled sessions file");
        Ok(())
    }
}

impl ManagedTask for SessionsBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

pub struct BurnedBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
    writer: BurnedDataTransferWriter,
}

impl BurnedBackfiller {
    pub async fn create_managed_task(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: BurnedDataTransferWriter,
        options: BackfillOptions,
    ) -> anyhow::Result<impl ManagedTask> {
        let mut builder = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(options.start_after)
            .process_name(format!("{}-burned", options.process_name))
            .idle_timeout(options.idle_timeout);

        if let Some(stop_after) = options.stop_after {
            builder = builder.stop_after(stop_after);
        }

        let (burned_reports, burned_reports_server) = builder.create().await?;
        let burned_backfiller = BurnedBackfiller::new(pool, burned_reports, writer);

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
                    tracing::info!(file = %file_info_stream.file_info, "received file");
                    self.handle_file(file_info_stream).await?;
                }
            }
        }
    }

    async fn handle_file(&self, file: FileInfoStream<ValidDataTransferSession>) -> Result<()> {
        tracing::info!(file = %file.file_info, "backfilling burned sessions file to iceberg");

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

        tracing::info!(count, "backfilled burned sessions file");
        Ok(())
    }
}

impl ManagedTask for BurnedBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
