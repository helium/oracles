use crate::{
    burner::Burner, event_ids::EventIdPurger, pending_burns, settings::Settings,
    MobileConfigClients, MobileConfigResolverExt,
};
use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore, FileType,
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
    ) -> futures::future::LocalBoxFuture<'static, Result<()>> {
        Box::pin(self.run(shutdown))
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
                    match self.burner.burn(&self.pool).await {
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
                    let Some(file) = file else {
                        anyhow::bail!("FileInfoPoller sender was dropped unexpectedly");
                    };
                    tracing::info!("Verifying file: {}", file.file_info);
                    let ts = file.file_info.timestamp;
                    let mut transaction = self.pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;
                    crate::accumulate::accumulate_sessions(&self.mobile_config_resolver, &mut transaction, &self.verified_data_session_report_sink, ts, reports).await?;
                    transaction.commit().await?;
                    self.verified_data_session_report_sink.commit().await?;
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

        pending_burns::initialize(&pool).await?;

        // Set up the solana network:
        let solana = if settings.enable_solana_integration {
            let Some(ref solana_settings) = settings.solana else {
                bail!("Missing solana section in settings");
            };
            // Set up the solana RpcClient:
            Some(SolanaRpc::new(solana_settings, solana::SubDao::Mobile).await?)
        } else {
            None
        };

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let (valid_sessions, valid_sessions_server) = ValidDataTransferSession::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (invalid_sessions, invalid_sessions_server) =
            VerifiedDataTransferIngestReportV1::file_sink(
                store_base_path,
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let burner = Burner::new(valid_sessions, solana);

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (reports, reports_server) =
            file_source::continuous_source::<DataTransferSessionIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(
                    Utc.timestamp_millis_opt(0).unwrap(),
                ))
                .prefix(FileType::DataTransferSessionIngestReport.to_string())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
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

        let event_id_purger = EventIdPurger::from_settings(pool, settings);

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_sessions_server)
            .add_task(invalid_sessions_server)
            .add_task(reports_server)
            .add_task(event_id_purger)
            .add_task(daemon)
            .build()
            .start()
            .await
    }
}
