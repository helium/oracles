use crate::iceberg::{
    self, burned_session::IcebergBurnedDataTransferSession, session::IcebergDataTransferSession,
    BurnedDataTransferWriter, DataTransferWriter,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source};
use file_store_oracles::{
    mobile_session::DataTransferSessionIngestReport, mobile_transfer::ValidDataTransferSession,
    FileType,
};
use futures::StreamExt;
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::settings::Settings;

#[derive(Debug, clap::Subcommand)]
pub enum BackfillTarget {
    /// Backfill DataTransferSessionIngestReport files to Iceberg sessions table
    Sessions,
    /// Backfill ValidDataTransferSession files to Iceberg burned_sessions table
    Burned,
    /// Backfill both sessions and burned_sessions tables
    All,
}

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Which tables to backfill
    #[clap(subcommand)]
    target: BackfillTarget,

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

        match self.target {
            BackfillTarget::Sessions => {
                self.run_sessions_backfill(settings, pool, session_writer)
                    .await
            }
            BackfillTarget::Burned => {
                self.run_burned_backfill(settings, pool, burned_session_writer)
                    .await
            }
            BackfillTarget::All => {
                self.run_all_backfills(settings, pool, session_writer, burned_session_writer)
                    .await
            }
        }
    }

    async fn run_sessions_backfill(
        &self,
        settings: &Settings,
        pool: Pool<Postgres>,
        writer: DataTransferWriter,
    ) -> Result<()> {
        tracing::info!(
            process_name = %self.process_name,
            stop_after = %self.stop_after,
            "starting sessions backfill"
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .stop_after(self.stop_after)
            .process_name(self.process_name.clone())
            .create()
            .await?;

        let backfiller = SessionsBackfiller::new(pool, reports, writer);

        TaskManager::builder()
            .add_task(reports_server)
            .add_task(backfiller)
            .build()
            .start()
            .await?;

        Ok(())
    }

    async fn run_burned_backfill(
        &self,
        settings: &Settings,
        pool: Pool<Postgres>,
        writer: BurnedDataTransferWriter,
    ) -> Result<()> {
        let burned_process_name = format!("{}-burned", self.process_name);
        tracing::info!(
            process_name = %burned_process_name,
            stop_after = %self.stop_after,
            "starting burned sessions backfill"
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.output_bucket.connect().await)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(settings.start_after)
            .stop_after(self.stop_after)
            .process_name(burned_process_name)
            .create()
            .await?;

        let backfiller = BurnedBackfiller::new(pool, reports, writer);

        TaskManager::builder()
            .add_task(reports_server)
            .add_task(backfiller)
            .build()
            .start()
            .await?;

        Ok(())
    }

    async fn run_all_backfills(
        &self,
        settings: &Settings,
        pool: Pool<Postgres>,
        session_writer: DataTransferWriter,
        burned_writer: BurnedDataTransferWriter,
    ) -> Result<()> {
        tracing::info!(
            process_name = %self.process_name,
            stop_after = %self.stop_after,
            "starting all backfills"
        );

        // Sessions backfill setup
        let (sessions_reports, sessions_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .stop_after(self.stop_after)
            .process_name(self.process_name.clone())
            .create()
            .await?;

        let sessions_backfiller =
            SessionsBackfiller::new(pool.clone(), sessions_reports, session_writer);

        // Burned sessions backfill setup
        let burned_process_name = format!("{}-burned", self.process_name);
        let (burned_reports, burned_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.output_bucket.connect().await)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(settings.start_after)
            .stop_after(self.stop_after)
            .process_name(burned_process_name)
            .create()
            .await?;

        let burned_backfiller = BurnedBackfiller::new(pool, burned_reports, burned_writer);

        // Run both pipelines under a single TaskManager
        TaskManager::builder()
            .add_task(sessions_server)
            .add_task(sessions_backfiller)
            .add_task(burned_server)
            .add_task(burned_backfiller)
            .build()
            .start()
            .await?;

        Ok(())
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

        let iceberg_sessions: Vec<IcebergDataTransferSession> = reports
            .map(IcebergDataTransferSession::from)
            .collect()
            .await;

        let count = iceberg_sessions.len();
        iceberg_txn
            .write(iceberg_sessions)
            .await
            .context("writing to iceberg")?;

        txn.commit().await.context("committing db transaction")?;
        iceberg_txn
            .publish()
            .await
            .context("publishing to iceberg")?;

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

        let iceberg_sessions: Vec<IcebergBurnedDataTransferSession> = reports
            .map(IcebergBurnedDataTransferSession::from)
            .collect()
            .await;

        let count = iceberg_sessions.len();
        iceberg_txn
            .write(iceberg_sessions)
            .await
            .context("writing to iceberg")?;

        txn.commit().await.context("committing db transaction")?;
        iceberg_txn
            .publish()
            .await
            .context("publishing to iceberg")?;

        tracing::info!(count, "backfilled burned sessions file");
        Ok(())
    }
}

impl ManagedTask for BurnedBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
