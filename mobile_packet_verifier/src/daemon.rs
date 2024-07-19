use crate::{burner::Burner, event_ids::EventIdPurger, settings::Settings};
use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport,
    FileSinkBuilder, FileStore, FileType,
};

use helium_proto::services::poc_mobile::InvalidDataTransferIngestReportV1;
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
    AuthorizationClient, GatewayClient,
};
use solana::burn::{SolanaNetwork, SolanaRpc};
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

pub struct Daemon<S, GIR, AV> {
    pool: Pool<Postgres>,
    burner: Burner<S>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    burn_period: Duration,
    min_burn_period: Duration,
    gateway_info_resolver: GIR,
    authorization_verifier: AV,
    invalid_data_session_report_sink: FileSinkClient<InvalidDataTransferIngestReportV1>,
}

impl<S, GIR, AV> Daemon<S, GIR, AV> {
    pub fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        burner: Burner<S>,
        gateway_info_resolver: GIR,
        authorization_verifier: AV,
        invalid_data_session_report_sink: FileSinkClient<InvalidDataTransferIngestReportV1>,
    ) -> Self {
        Self {
            pool,
            burner,
            reports,
            burn_period: settings.burn_period,
            min_burn_period: settings.min_burn_period,
            gateway_info_resolver,
            authorization_verifier,
            invalid_data_session_report_sink,
        }
    }
}

impl<S, GIR, AV> ManagedTask for Daemon<S, GIR, AV>
where
    S: SolanaNetwork,
    GIR: GatewayInfoResolver,
    AV: AuthorizationVerifier + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl<S, GIR, AV> Daemon<S, GIR, AV>
where
    S: SolanaNetwork,
    GIR: GatewayInfoResolver,
    AV: AuthorizationVerifier,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        // Set the initial burn period to one minute
        let mut burn_time = Instant::now() + Duration::from_secs(60);
        loop {
            tokio::select! {
                file = self.reports.recv() => {
                    let Some(file) = file else {
                        anyhow::bail!("FileInfoPoller sender was dropped unexpectedly");
                    };
                    tracing::info!("Verifying file: {}", file.file_info);
                    let ts = file.file_info.timestamp;
                    let mut transaction = self.pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;
                    crate::accumulate::accumulate_sessions(&self.gateway_info_resolver, &self.authorization_verifier, &mut transaction, &self.invalid_data_session_report_sink, ts, reports).await?;
                    transaction.commit().await?;
                    self.invalid_data_session_report_sink.commit().await?;
                },
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
                _ = shutdown.clone() => return Ok(()),
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

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let (valid_sessions, valid_sessions_server) = FileSinkBuilder::new(
            FileType::ValidDataTransferSession,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_valid_data_transfer_session"),
        )
        .auto_commit(true)
        .create()
        .await?;

        let (invalid_sessions, invalid_sessions_server) = FileSinkBuilder::new(
            FileType::InvalidDataTransferSessionIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_invalid_data_transfer_session"),
        )
        .auto_commit(false)
        .create()
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

        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;

        let daemon = Daemon::new(
            settings,
            pool.clone(),
            reports,
            burner,
            gateway_client,
            auth_client,
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
