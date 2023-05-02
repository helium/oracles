use crate::{burner::Burner, settings::Settings};
use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport,
    FileSinkBuilder, FileStore, FileType,
};
use futures::future::LocalBoxFuture;
use mobile_config::Client;
use solana::{balance_monitor::BalanceMonitor, SolanaNetwork, SolanaRpc};
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub struct Daemon<S> {
    pool: Pool<Postgres>,
    burner: Burner<S>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    burn_period: Duration,
    config_client: Client,
}

impl<S> Daemon<S> {
    pub fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        burner: Burner<S>,
        config_client: Client,
    ) -> Self {
        Self {
            pool,
            burner,
            reports,
            burn_period: Duration::from_secs(60 * 60 * settings.burn_period as u64),
            config_client,
        }
    }
}

impl<S> ManagedTask for Daemon<S>
where
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(token))
    }
}

impl<S> Daemon<S>
where
    S: SolanaNetwork,
{
    pub async fn run(mut self, token: CancellationToken) -> Result<()> {
        // Set the initial burn period to one minute
        let mut burn_time = Instant::now() + Duration::from_secs(60);
        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok(()),
                file = self.reports.recv() => {
                    let Some(file) = file else {
                        anyhow::bail!("FileInfoPoller sender was dropped unexpectedly");
                    };
                    tracing::info!("Verifying file: {}", file.file_info);
                    let ts = file.file_info.timestamp;
                    let mut transaction = self.pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;
                    crate::accumulate::accumulate_sessions(&mut self.config_client, &mut transaction, ts, reports).await?;
                    transaction.commit().await?;
                },
                _ = sleep_until(burn_time) => {
                    // It's time to burn
                    self.burner.burn(&self.pool).await?;
                    burn_time = Instant::now() + self.burn_period;
                }
            }
        }
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

        let sol_balance_monitor = BalanceMonitor::new(env!("CARGO_PKG_NAME"), solana.clone())?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let (valid_sessions, valid_sessions_server) = FileSinkBuilder::new(
            FileType::ValidDataTransferSession,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_invalid_packets"),
        )
        .file_upload(Some(file_upload))
        .auto_commit(true)
        .create()
        .await?;

        let burner = Burner::new(valid_sessions, solana);

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (reports, reports_server) =
            file_source::continuous_source::<DataTransferSessionIngestReport>()
                .db(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(
                    Utc.timestamp_millis_opt(0).unwrap(),
                ))
                .file_type(FileType::DataTransferSessionIngestReport)
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .create()?;

        let config_client = Client::from_settings(&settings.config_client)?;

        let daemon = Daemon::new(settings, pool, reports, burner, config_client);

        TaskManager::builder()
            .add(file_upload_server)
            .add(valid_sessions_server)
            .add(reports_server)
            .add(sol_balance_monitor)
            .add(daemon)
            .start()
            .await
    }
}
