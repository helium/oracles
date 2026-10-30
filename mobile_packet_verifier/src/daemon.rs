use crate::{
    accumulate::{accumulate_sessions, AccumulatedSessions},
    banning::{self, BannedRadios},
    burner::Burner,
    event_ids::EventIdPurger,
    gateway::GatewayResolver,
    iceberg::{self, DataTransferWriter, InvalidDataTransferWriter},
    multiplier, pending_burns,
    routing::RoutingKeys,
    settings::Settings,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source};
use file_store_oracles::{mobile_session::DataTransferSessionIngestReport, FileType};
use futures::Stream;
use solana::burn::{SolanaNetwork, SolanaRpc};
use sqlx::{Pool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

// This enum exists to help with testing the Daemon.
//
// Tests hook into the runtime of the Daemon by watching for the events it has
// done and making decisions based off them, such as waiting for a
// DaemonEvent::BurnSuccess before checking what was written to Iceberg.
#[derive(Debug, Clone, Copy)]
pub enum DaemonEvent {
    BurnSuccess,
    BurnFailure,
    ReportHandle,
}

pub struct Daemon<S> {
    pool: Pool<Postgres>,
    burner: Burner<S>,
    burn_period: Duration,
    min_burn_period: Duration,
    initial_burn_delay: Duration,
    ingest_reports: IngestReports,
    event_tx: tokio::sync::broadcast::Sender<DaemonEvent>,
}

impl<S> Daemon<S> {
    pub fn new(
        pool: Pool<Postgres>,
        burn_period: Duration,
        min_burn_period: Duration,
        initial_burn_delay: Duration,
        ingest_reports: IngestReports,
        burner: Burner<S>,
    ) -> Self {
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(100);
        Self {
            pool,
            burner,
            burn_period,
            min_burn_period,
            initial_burn_delay,
            ingest_reports,
            event_tx,
        }
    }
}

impl<S> ManagedTask for Daemon<S>
where
    S: SolanaNetwork,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<S> Daemon<S>
where
    S: SolanaNetwork,
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
                    self.ingest_reports.handle(file).await?;
                    let _ = self.event_tx.send(DaemonEvent::ReportHandle);
                }
            }
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub async fn handle_data_transfer_session_file(
    txn: &mut Transaction<'_, Postgres>,
    iceberg_writer: &iceberg::DataTransferWriter,
    invalid_iceberg_writer: &iceberg::InvalidDataTransferWriter,
    write_id: &str,
    banned_radios: BannedRadios,
    resolver: &GatewayResolver,
    routing_keys: &RoutingKeys,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    let sessions = accumulate_sessions(
        resolver,
        routing_keys,
        banned_radios,
        txn,
        reports,
        curr_file_ts,
    )
    .await
    .context("accumulating sessions")?;

    let AccumulatedSessions {
        iceberg_sessions,
        invalid_iceberg_sessions,
        db_sessions,
    } = sessions;

    pending_burns::save_data_transfer_sessions(txn, &db_sessions)
        .await
        .context("saving to db")?;

    iceberg_writer
        .write_idempotent(write_id, iceberg_sessions)
        .await
        .context("writing data sessions")?;

    invalid_iceberg_writer
        .write_idempotent(write_id, invalid_iceberg_sessions)
        .await
        .context("writing invalid data sessions")?;

    Ok(())
}

/// Handles the real-time `DataTransferSessionIngestReport` stream for the daemon.
///
/// The Daemon holds one of these, calls `recv()` in its `select!`, and forwards
/// each file to `handle`. The channel is never expected to close while the daemon
/// is running — a dropped sender is an error.
///
/// Holds the inputs used to verify each report: the `gateway_resolver` (whose
/// snapshot is refreshed by a separate `GatewaySnapshotRefresher` task on the
/// top-level `TaskManager`) and the static `routing_keys` allow-list.
pub struct IngestReports {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    iceberg_writer: DataTransferWriter,
    invalid_iceberg_writer: InvalidDataTransferWriter,
    gateway_resolver: GatewayResolver,
    routing_keys: RoutingKeys,
}

impl IngestReports {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        iceberg_writer: DataTransferWriter,
        invalid_iceberg_writer: InvalidDataTransferWriter,
        gateway_resolver: GatewayResolver,
        routing_keys: RoutingKeys,
    ) -> Self {
        Self {
            pool,
            reports,
            iceberg_writer,
            invalid_iceberg_writer,
            gateway_resolver,
            routing_keys,
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
    ) -> anyhow::Result<()> {
        let Some(file_info_stream) = file else {
            anyhow::bail!("data transfer FileInfoPoller sender was dropped unexpectedly");
        };
        self.handle_file(file_info_stream).await
    }

    async fn handle_file(
        &self,
        file: FileInfoStream<DataTransferSessionIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!("Verifying file: {}", file.file_info);
        let ts = file.file_info.timestamp;
        let write_id = file.file_info.key.clone();
        let mut transaction = self.pool.begin().await?;

        let banned_radios = banning::get_banned_radios(&mut transaction, ts).await?;
        let reports = file.into_stream(&mut transaction).await?;

        handle_data_transfer_session_file(
            &mut transaction,
            &self.iceberg_writer,
            &self.invalid_iceberg_writer,
            &write_id,
            banned_radios,
            &self.gateway_resolver,
            &self.routing_keys,
            ts,
            reports,
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct Cmd;

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

        let writers = iceberg::get_writers(&settings.iceberg_settings).await?;
        let session_writer = writers.session;
        let invalid_session_writer = writers.invalid_session;
        let burned_session_writer = writers.burned_session;
        let multiplier_ticket_writer = writers.multiplier_ticket;

        let burner = Burner::new(
            solana,
            settings.txn_confirmation_retry_attempts,
            settings.txn_confirmation_check_interval,
            burned_session_writer,
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(settings.ingest_bucket.connect().await)
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .create()
            .await?;

        let gw_resolver = GatewayResolver::new(
            trino_client::Client::from_settings(&settings.trino)?,
            settings.gateway_refresh_interval,
        )
        .await;
        let gw_refresher = gw_resolver.refresher();
        // Shared, not a second resolver: clones point at the same snapshot, so
        // the refresher above keeps the ticket path current too. Building
        // another would load its own copy of the inventory and never refresh it.
        let ticket_resolver = gw_resolver.clone();

        let ingest_reports = IngestReports::new(
            pool.clone(),
            reports,
            session_writer,
            invalid_session_writer,
            gw_resolver,
            settings.routing_keys()?,
        );

        let daemon = Daemon::new(
            pool.clone(),
            settings.burn_period,
            settings.min_burn_period,
            Duration::from_secs(60),
            ingest_reports,
            burner,
        );

        let event_id_purger = EventIdPurger::from_settings(pool.clone(), settings);
        let banning = banning::create_managed_task(pool.clone(), &settings.banning).await?;

        let multipliers = multiplier::create_managed_task(
            pool,
            &settings.multiplier,
            settings.ticket_signers()?,
            ticket_resolver,
            multiplier_ticket_writer,
        )
        .await?;

        TaskManager::builder()
            .add_task(reports_server)
            .add_task(banning)
            .add_task(multipliers)
            .add_task(task_manager::periodic(event_id_purger))
            .add_task(task_manager::periodic(gw_refresher))
            .add_task(daemon)
            .build()
            .start()
            .await?;
        Ok(())
    }
}
