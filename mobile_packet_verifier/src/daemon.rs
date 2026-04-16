use crate::{
    accumulate::{accumulate_sessions, AccumulatedSessions},
    backfill::{BackfillOptions, BurnedSessionsBackfiller, DataSessionsBackfiller},
    banning::{self, BannedRadios},
    burner::Burner,
    event_ids::EventIdPurger,
    iceberg::{self, DataTransferWriter},
    pending_burns,
    settings::Settings,
    MobileConfigClients, MobileConfigResolverExt,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source, file_upload,
};
use file_store_oracles::{
    mobile_session::DataTransferSessionIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use futures::Stream;
use helium_proto::services::{
    packet_verifier::ValidDataTransferSession, poc_mobile::VerifiedDataTransferIngestReportV1,
};
use solana::burn::{SolanaNetwork, SolanaRpc};
use sqlx::{Pool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

// This enum exists to help with testing the Daemon.
//
// Normal setup for this service is for burned sessions to be part of a
// FileSinkClient that commits itself automatically. The timing for that commit
// is far beyond what we want in testing. We can allow tests to hook into the
// runtime of Daemon by providing Events that is has done, and make decisions
// based off those events. The easiest example of that is waiting for a
// DaemonEvent::BurnSuccess and manually calling sink.commit() in a test to
// ensure the correct items were written to s3.
#[derive(Debug, Clone, Copy)]
pub enum DaemonEvent {
    BurnSuccess,
    BurnFailure,
    ReportHandle,
    BackfillDataSession,
    BackfillBurnedSession,
}

pub struct Daemon<S, MCR> {
    pool: Pool<Postgres>,
    burner: Burner<S>,
    burn_period: Duration,
    min_burn_period: Duration,
    initial_burn_delay: Duration,
    mobile_config_resolver: MCR,
    ingest_reports: IngestReports,
    event_tx: tokio::sync::broadcast::Sender<DaemonEvent>,
    // backfill temp
    session_backfill: DataSessionsBackfiller,
    burned_backfill: BurnedSessionsBackfiller,
}

impl<S, MCR> Daemon<S, MCR> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<Postgres>,
        burn_period: Duration,
        min_burn_period: Duration,
        initial_burn_delay: Duration,
        ingest_reports: IngestReports,
        burner: Burner<S>,
        mobile_config_resolver: MCR,
        // backfill temp
        session_backfill: DataSessionsBackfiller,
        burned_backfill: BurnedSessionsBackfiller,
    ) -> Self {
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(100);
        Self {
            pool,
            burner,
            burn_period,
            min_burn_period,
            initial_burn_delay,
            mobile_config_resolver,
            ingest_reports,
            session_backfill,
            burned_backfill,
            event_tx,
        }
    }
}

impl<S, MCR> ManagedTask for Daemon<S, MCR>
where
    S: SolanaNetwork,
    MCR: MobileConfigResolverExt + 'static,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<S, MCR> Daemon<S, MCR>
where
    S: SolanaNetwork,
    MCR: MobileConfigResolverExt,
{
    pub fn event_rx(&self) -> tokio::sync::broadcast::Receiver<DaemonEvent> {
        self.event_tx.subscribe()
    }

    pub async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        let mut burn_time = Instant::now() + self.initial_burn_delay;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = sleep_until(burn_time) => {
                    match self.burner.confirm_and_burn(&self.pool).await {
                        Ok(_) => {
                            let _ = self.event_tx.send(DaemonEvent::BurnSuccess);
                            burn_time = Instant::now() + self.burn_period;
                        }
                        Err(e) => {
                            let _ = self.event_tx.send(DaemonEvent::BurnFailure);
                            burn_time = Instant::now() + self.min_burn_period;
                            tracing::warn!("failed to burn {e:?}, retrying in {:?}", self.min_burn_period);
                        }
                    }
                }
                file = self.ingest_reports.recv() => {
                    self.ingest_reports.handle(file, &self.mobile_config_resolver).await?;
                    let _ = self.event_tx.send(DaemonEvent::ReportHandle);
                }
                // Backfill runs at lowest priority — only fires when burn and
                // ingest have nothing ready. When iceberg is not configured,
                // the backfillers set done=true on construction and recv()
                // returns pending() immediately, so these arms never fire.
                file = self.session_backfill.recv() => {
                    self.session_backfill.handle(file).await?;
                    let _ = self.event_tx.send(DaemonEvent::BackfillDataSession);
                }
                file = self.burned_backfill.recv() => {
                    self.burned_backfill.handle(file).await?;
                    let _ = self.event_tx.send(DaemonEvent::BackfillBurnedSession);
                }
            }
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub async fn handle_data_transfer_session_file(
    txn: &mut Transaction<'_, Postgres>,
    iceberg_writer: Option<&iceberg::DataTransferWriter>,
    write_id: &str,
    banned_radios: BannedRadios,
    mobile_config: &impl MobileConfigResolverExt,
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    let sessions = accumulate_sessions(mobile_config, banned_radios, txn, reports, curr_file_ts)
        .await
        .context("accumulating sessions")?;

    let AccumulatedSessions {
        iceberg_sessions,
        proto_sessions,
        db_sessions,
    } = sessions;

    pending_burns::save_data_transfer_sessions(txn, &db_sessions)
        .await
        .context("saving to db")?;

    for session in proto_sessions {
        let status = session.status().as_str_name();
        verified_data_session_report_sink
            .write(session, &[("status", status)])
            .await
            .context("writing to file-store")?;
    }

    iceberg::maybe_write_idempotent(iceberg_writer, write_id, iceberg_sessions).await?;

    Ok(())
}

/// Handles the real-time `DataTransferSessionIngestReport` stream for the daemon.
///
/// Mirrors the shape of [`DataSessionsBackfiller`] and [`BurnedSessionsBackfiller`]:
/// the Daemon holds one of these, calls `recv()` in its `select!`, and forwards the
/// result to `handle(file, mobile_config)`.  Unlike the backfillers, the channel is
/// never expected to close while the daemon is running — a dropped sender is an error.
///
/// The `mobile_config_resolver` is intentionally **not** stored here; the Daemon
/// passes `&self.mobile_config_resolver` at each call site so ownership stays in one place.
pub struct IngestReports {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    verified_sink: FileSinkClient<VerifiedDataTransferIngestReportV1>,
    iceberg_writer: Option<DataTransferWriter>,
}

impl IngestReports {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        verified_sink: FileSinkClient<VerifiedDataTransferIngestReportV1>,
        iceberg_writer: Option<DataTransferWriter>,
    ) -> Self {
        Self {
            pool,
            reports,
            verified_sink,
            iceberg_writer,
        }
    }

    pub async fn recv(&mut self) -> Option<FileInfoStream<DataTransferSessionIngestReport>> {
        self.reports.recv().await
    }

    /// Process one file from the ingest stream.
    ///
    /// Returns `Err` if `file` is `None`, which means the upstream sender was dropped
    /// — an unexpected condition that should propagate as a task failure.
    pub async fn handle(
        &self,
        file: Option<FileInfoStream<DataTransferSessionIngestReport>>,
        mobile_config: &impl MobileConfigResolverExt,
    ) -> anyhow::Result<()> {
        let Some(file_info_stream) = file else {
            anyhow::bail!("data transfer FileInfoPoller sender was dropped unexpectedly");
        };
        self.handle_file(file_info_stream, mobile_config).await
    }

    async fn handle_file(
        &self,
        file: FileInfoStream<DataTransferSessionIngestReport>,
        mobile_config: &impl MobileConfigResolverExt,
    ) -> anyhow::Result<()> {
        tracing::info!("Verifying file: {}", file.file_info);
        let ts = file.file_info.timestamp;
        let mut transaction = self.pool.begin().await?;

        let mut iceberg_txn =
            iceberg::maybe_begin(self.iceberg_writer.as_ref(), file.file_info.as_ref()).await?;

        let banned_radios = banning::get_banned_radios(&mut transaction, ts).await?;
        let reports = file.into_stream(&mut transaction).await?;

        handle_data_transfer_session_file(
            &mut transaction,
            iceberg_txn.as_mut(),
            banned_radios,
            mobile_config,
            &self.verified_sink,
            ts,
            reports,
        )
        .await?;

        iceberg::maybe_publish(iceberg_txn).await?;
        transaction.commit().await?;
        self.verified_sink.commit().await?;

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        // Set up the postgres pool:
        let pool = settings.database.connect("mobile-packet-verifier").await?;
        sqlx::migrate!().run(&pool).await?;

        pending_burns::initialize(&pool).await?;

        // Set up the solana network:
        let solana = if settings.enable_solana_integration {
            let Some(ref solana_settings) = settings.solana else {
                bail!("Missing solana section in settings");
            };
            // Set up the solana RpcClient:
            Some(SolanaRpc::new(solana_settings).await?)
        } else {
            None
        };

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_bucket_client(settings.output_bucket.connect().await)
                .await;

        let (valid_sessions, valid_sessions_server) = ValidDataTransferSession::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (verified_sessions, verified_sessions_server) =
            VerifiedDataTransferIngestReportV1::file_sink(
                &settings.cache,
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (session_writer, burned_session_writer) =
            if let Some(ref iceberg_settings) = settings.iceberg_settings {
                tracing::info!("iceberg settings provided, connecting...");
                let (session, burned) = iceberg::get_writers(iceberg_settings).await?;
                (Some(session), Some(burned))
            } else {
                tracing::info!("no iceberg settings provided");
                (None, None)
            };

        let burner = Burner::new(
            valid_sessions,
            solana,
            settings.txn_confirmation_retry_attempts,
            settings.txn_confirmation_check_interval,
            burned_session_writer.clone(),
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .create()
            .await?;

        let resolver = MobileConfigClients::new(&settings.config_client)?;

        let ingest_reports = IngestReports::new(
            pool.clone(),
            reports,
            verified_sessions,
            session_writer.clone(),
        );

        // Backfill covers [backfill.start_after, backfill.stop_after):
        //   - backfill.stop_after is the Iceberg deployment date (set at deploy time)
        //   - everything before stop_after is historical data written by the backfiller
        //   - everything from stop_after onwards is written to Iceberg in real-time
        // This boundary is deterministic and configured at deploy time — no overlap.
        let backfill_opts = BackfillOptions::new(
            "backfill",
            settings.backfill.start_after,
            settings.backfill.stop_after,
        );

        let (data_session_backfill, backfill_reports_server) = DataSessionsBackfiller::create(
            pool.clone(),
            settings.output_bucket.connect().await,
            session_writer,
            backfill_opts.clone(),
        )
        .await?;
        let (burned_session_backfill, burned_reports_server) = BurnedSessionsBackfiller::create(
            pool.clone(),
            settings.output_bucket.connect().await,
            burned_session_writer.clone(),
            backfill_opts,
        )
        .await?;

        let daemon = Daemon::new(
            pool.clone(),
            settings.burn_period,
            settings.min_burn_period,
            Duration::from_secs(60),
            ingest_reports,
            burner,
            resolver,
            data_session_backfill,
            burned_session_backfill,
        );

        let event_id_purger = EventIdPurger::from_settings(pool.clone(), settings);
        let banning = banning::create_managed_task(pool, &settings.banning).await?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_sessions_server)
            .add_task(verified_sessions_server)
            .add_task(reports_server)
            .add_task(banning)
            .add_task(event_id_purger)
            .add_task(daemon)
            // temp backfill
            .add_task(backfill_reports_server)
            .add_task(burned_reports_server)
            .build()
            .start()
            .await?;
        Ok(())
    }
}
