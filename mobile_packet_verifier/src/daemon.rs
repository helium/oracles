use crate::{
    banning::{self},
    burner::Burner,
    event_ids::EventIdPurger,
    pending_burns,
    settings::Settings,
    MobileConfigClients, MobileConfigResolverExt,
};
use anyhow::{bail, Result};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};

use helium_proto::services::{
    packet_verifier::ValidDataTransferSession, poc_mobile::VerifiedDataTransferIngestReportV1,
};
use solana::burn::{SolanaNetwork, SolanaRpc};
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

pub struct Daemon<S, MCR> {
    pool: Pool<Postgres>,
    burner: Burner<S>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    burn_period: Duration,
    min_burn_period: Duration,
    mobile_config_resolver: MCR,
    verified_data_session_report_sink: FileSinkClient<VerifiedDataTransferIngestReportV1>,
}

impl<S, MCR> Daemon<S, MCR> {
    pub fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        burner: Burner<S>,
        mobile_config_resolver: MCR,
        verified_data_session_report_sink: FileSinkClient<VerifiedDataTransferIngestReportV1>,
    ) -> Self {
        Self {
            pool,
            burner,
            reports,
            burn_period: settings.burn_period,
            min_burn_period: settings.min_burn_period,
            mobile_config_resolver,
            verified_data_session_report_sink,
        }
    }
}

impl<S, MCR> ManagedTask for Daemon<S, MCR>
where
    S: SolanaNetwork,
    MCR: MobileConfigResolverExt + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::run(self.run(shutdown))
    }
}

impl<S, MCR> Daemon<S, MCR>
where
    S: SolanaNetwork,
    MCR: MobileConfigResolverExt,
{
    pub async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        // Set the initial burn period to one minute
        let mut burn_time = Instant::now() + Duration::from_secs(60);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = sleep_until(burn_time) => {
                    // It's time to burn
                    match self.burner.confirm_and_burn(&self.pool).await {
                        Ok(_) => {
                            burn_time = Instant::now() + self.burn_period;
                        }
                        Err(e) => {
                            burn_time = Instant::now() + self.min_burn_period;
                            tracing::warn!("failed to burn {e:?}, re running burn in {:?} min", self.min_burn_period);
                        }
                    }
                }
                file = self.reports.recv() => {
                    let Some(file_info_stream) = file else {
                        anyhow::bail!("data transfer FileInfoPoller sender was dropped unexpectedly");
                    };
                    self.handle_data_transfer_session_file(file_info_stream).await?;
                }

            }
        }
    }

    async fn handle_data_transfer_session_file(
        &self,
        file: FileInfoStream<DataTransferSessionIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!("Verifying file: {}", file.file_info);
        let ts = file.file_info.timestamp;
        let mut transaction = self.pool.begin().await?;

        let banned_radios = banning::get_banned_radios(&mut transaction, ts).await?;
        let reports = file.into_stream(&mut transaction).await?;

        crate::accumulate::accumulate_sessions(
            &self.mobile_config_resolver,
            banned_radios,
            &mut transaction,
            &self.verified_data_session_report_sink,
            ts,
            reports,
        )
        .await?;

        transaction.commit().await?;
        self.verified_data_session_report_sink.commit().await?;
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

        let file_store_client = settings.file_store.connect().await;
        let (file_upload, file_upload_server) =
            file_upload::FileUpload::new(file_store_client.clone(), settings.output_bucket.clone())
                .await;

        let (valid_sessions, valid_sessions_server) = ValidDataTransferSession::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (invalid_sessions, invalid_sessions_server) =
            VerifiedDataTransferIngestReportV1::file_sink(
                &settings.cache,
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let burner = Burner::new(
            valid_sessions,
            solana,
            settings.txn_confirmation_retry_attempts,
            settings.txn_confirmation_check_interval,
        );

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .file_store(file_store_client.clone(), settings.ingest_bucket.clone())
            .prefix(FileType::DataTransferSessionIngestReport.to_string())
            .lookback_start_after(settings.start_after)
            .create()
            .await?;

        let resolver = MobileConfigClients::new(&settings.config_client)?;

        let daemon = Daemon::new(
            settings,
            pool.clone(),
            reports,
            burner,
            resolver,
            invalid_sessions,
        );

        let event_id_purger = EventIdPurger::from_settings(pool.clone(), settings);
        let banning =
            banning::create_managed_task(pool, file_store_client, &settings.banning).await?;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_sessions_server)
            .add_task(invalid_sessions_server)
            .add_task(reports_server)
            .add_task(banning)
            .add_task(event_id_purger)
            .add_task(daemon)
            .build()
            .start()
            .await
    }
}
