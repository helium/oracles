use crate::{
    burner::{BurnConfirmer, BurnInitiator},
    event_ids::EventIdPurger,
    settings::Settings,
};
use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport,
    FileSinkBuilder, FileStore, FileType,
};

use mobile_config::client::{
    authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
    AuthorizationClient, GatewayClient,
};
use solana::SolanaRpc;
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::{sync::mpsc::Receiver, time::Duration};

pub struct Daemon<GIR, AV> {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    gateway_info_resolver: GIR,
    authorization_verifier: AV,
    invalid_data_session_report_sink: FileSinkClient,
}

impl<GIR, AV> Daemon<GIR, AV> {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        gateway_info_resolver: GIR,
        authorization_verifier: AV,
        invalid_data_session_report_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            reports,
            gateway_info_resolver,
            authorization_verifier,
            invalid_data_session_report_sink,
        }
    }
}

impl<GIR, AV> ManagedTask for Daemon<GIR, AV>
where
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

impl<GIR, AV> Daemon<GIR, AV>
where
    GIR: GatewayInfoResolver,
    AV: AuthorizationVerifier,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
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
            concat!(env!("CARGO_PKG_NAME"), "_valid_data_transfer_session"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(true)
        .create()
        .await?;

        let burn_initiator = BurnInitiator::new(
            solana.clone(),
            pool.clone(),
            Duration::from_secs(60 * 60 * settings.burn_period),
        );

        let burn_confirmer = BurnConfirmer::new(
            valid_sessions,
            solana,
            pool.clone(),
            Duration::from_secs(60 * settings.confirmation_period),
        );

        let (invalid_sessions, invalid_sessions_server) = FileSinkBuilder::new(
            FileType::InvalidDataTransferSessionIngestReport,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_invalid_data_transfer_session"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (reports, reports_server) =
            file_source::continuous_source::<DataTransferSessionIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(
                    Utc.timestamp_millis_opt(0).unwrap(),
                ))
                .prefix(FileType::DataTransferSessionIngestReport.to_string())
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .create()?;

        let gateway_client = GatewayClient::from_settings(&settings.config_client)?;
        let auth_client = AuthorizationClient::from_settings(&settings.config_client)?;

        let daemon = Daemon::new(
            pool.clone(),
            reports,
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
            .add_task(burn_initiator)
            .add_task(burn_confirmer)
            .add_task(daemon)
            .start()
            .await
    }
}
