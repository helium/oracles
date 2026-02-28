use crate::iceberg::{
    self, burned_session::IcebergBurnedDataTransferSession, session::IcebergDataTransferSession,
    BurnedDataTransferWriter, DataTransferWriter,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source};
use file_store_oracles::{
    mobile_session::DataTransferSessionIngestReport, mobile_transfer::ValidDataTransferSession,
    FileType,
};
use futures::StreamExt;
use sqlx::{Pool, Postgres};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

use crate::settings::Settings;

/// Runs the poller server and backfiller until the backfiller completes.
/// When the backfiller finishes (either by hitting stop_at or completing all files),
/// it triggers shutdown which stops the poller server.
async fn run_until_done<T: ManagedTask + 'static>(
    poller_server: impl ManagedTask + 'static,
    backfiller: T,
    shutdown_listener: triggered::Listener,
) -> Result<()> {
    let (poller_trigger, poller_listener) = triggered::trigger();
    let mut poller_future = Box::new(poller_server).start_task(poller_listener);
    let backfiller_future = Box::new(backfiller).start_task(shutdown_listener.clone());

    // Wait for either the backfiller to complete or an external shutdown
    tokio::select! {
        result = backfiller_future => {
            // Backfiller finished, stop the poller
            poller_trigger.trigger();
            let _ = (&mut poller_future).await;
            result.map_err(Into::into)
        }
        result = &mut poller_future => {
            // Poller finished unexpectedly (shouldn't happen normally)
            result.map_err(Into::into)
        }
    }
}

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

    /// Stop processing files when their timestamp is >= this value.
    /// Use this to avoid reprocessing files that the daemon has already handled.
    /// Format: RFC 3339 (e.g., 2025-02-25T00:00:00Z)
    #[clap(long)]
    stop_at: Option<DateTime<Utc>>,
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
            "starting sessions backfill"
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .process_name(self.process_name.clone())
            .create()
            .await?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let backfiller =
            SessionsBackfiller::new(pool, reports, writer, self.stop_at, shutdown_trigger);

        run_until_done(reports_server, backfiller, shutdown_listener).await
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
            "starting burned sessions backfill"
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.output_bucket.connect().await)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(settings.start_after)
            .process_name(burned_process_name)
            .create()
            .await?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let backfiller =
            BurnedBackfiller::new(pool, reports, writer, self.stop_at, shutdown_trigger);

        run_until_done(reports_server, backfiller, shutdown_listener).await
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
            "starting all backfills"
        );

        // Sessions backfill setup
        let (sessions_reports, sessions_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .process_name(self.process_name.clone())
            .create()
            .await?;

        // Run sessions and burned backfills concurrently using separate run_until_done calls
        let (sessions_shutdown_trigger, sessions_shutdown_listener) = triggered::trigger();
        let sessions_backfiller = SessionsBackfiller::new(
            pool.clone(),
            sessions_reports,
            session_writer,
            self.stop_at,
            sessions_shutdown_trigger,
        );

        // Burned sessions backfill setup
        let burned_process_name = format!("{}-burned", self.process_name);
        let (burned_reports, burned_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.output_bucket.connect().await)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .lookback_start_after(settings.start_after)
            .process_name(burned_process_name)
            .create()
            .await?;

        let (burned_shutdown_trigger, burned_shutdown_listener) = triggered::trigger();
        let burned_backfiller = BurnedBackfiller::new(
            pool,
            burned_reports,
            burned_writer,
            self.stop_at,
            burned_shutdown_trigger,
        );

        // Run both backfills concurrently, each with its own poller
        let sessions_handle =
            run_until_done(sessions_server, sessions_backfiller, sessions_shutdown_listener);
        let burned_handle =
            run_until_done(burned_server, burned_backfiller, burned_shutdown_listener);

        let (sessions_result, burned_result) = tokio::join!(sessions_handle, burned_handle);
        sessions_result?;
        burned_result?;
        Ok(())
    }
}

pub struct SessionsBackfiller {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    writer: DataTransferWriter,
    stop_at: Option<DateTime<Utc>>,
    shutdown_trigger: triggered::Trigger,
}

impl SessionsBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        writer: DataTransferWriter,
        stop_at: Option<DateTime<Utc>>,
        shutdown_trigger: triggered::Trigger,
    ) -> Self {
        Self {
            pool,
            reports,
            writer,
            stop_at,
            shutdown_trigger,
        }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        let result = self.run_inner(&mut shutdown).await;
        self.shutdown_trigger.trigger();
        result
    }

    async fn run_inner(
        &mut self,
        shutdown: &mut triggered::Listener,
    ) -> Result<()> {
        tracing::info!(stop_at = ?self.stop_at, "sessions backfiller starting");
        loop {
            tokio::select! {
                biased;
                _ = &mut *shutdown => {
                    tracing::info!("sessions backfiller shutting down");
                    return Ok(());
                }
                file = self.reports.recv() => {
                    let Some(file_info_stream) = file else {
                        bail!("sessions FileInfoPoller sender was dropped unexpectedly");
                    };
                    tracing::info!(file = %file_info_stream.file_info, "received file");
                    if let Some(stop_at) = self.stop_at {
                        if file_info_stream.file_info.timestamp >= stop_at {
                            tracing::info!(
                                file = %file_info_stream.file_info,
                                %stop_at,
                                "reached stop_at timestamp, stopping sessions backfill"
                            );
                            return Ok(());
                        }
                    }
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

        let iceberg_sessions: Vec<IcebergDataTransferSession> =
            reports.map(IcebergDataTransferSession::from).collect().await;

        let count = iceberg_sessions.len();
        iceberg_txn
            .write(iceberg_sessions)
            .await
            .context("writing to iceberg")?;

        txn.commit().await.context("committing db transaction")?;
        iceberg_txn.publish().await.context("publishing to iceberg")?;

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
    stop_at: Option<DateTime<Utc>>,
    shutdown_trigger: triggered::Trigger,
}

impl BurnedBackfiller {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<ValidDataTransferSession>>,
        writer: BurnedDataTransferWriter,
        stop_at: Option<DateTime<Utc>>,
        shutdown_trigger: triggered::Trigger,
    ) -> Self {
        Self {
            pool,
            reports,
            writer,
            stop_at,
            shutdown_trigger,
        }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        let result = self.run_inner(&mut shutdown).await;
        self.shutdown_trigger.trigger();
        result
    }

    async fn run_inner(
        &mut self,
        shutdown: &mut triggered::Listener,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = &mut *shutdown => {
                    tracing::info!("burned backfiller shutting down");
                    return Ok(());
                }
                file = self.reports.recv() => {
                    let Some(file_info_stream) = file else {
                        bail!("burned FileInfoPoller sender was dropped unexpectedly");
                    };
                    if let Some(stop_at) = self.stop_at {
                        if file_info_stream.file_info.timestamp >= stop_at {
                            tracing::info!(
                                file = %file_info_stream.file_info,
                                %stop_at,
                                "reached stop_at timestamp, stopping burned sessions backfill"
                            );
                            return Ok(());
                        }
                    }
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
        iceberg_txn.publish().await.context("publishing to iceberg")?;

        tracing::info!(count, "backfilled burned sessions file");
        Ok(())
    }
}

impl ManagedTask for BurnedBackfiller {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}
