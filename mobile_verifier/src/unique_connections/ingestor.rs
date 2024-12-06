use chrono::Utc;
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    unique_connections::{
        UniqueConnectionReq, UniqueConnectionsIngestReport, VerifiedUniqueConnectionsIngestReport,
    },
    FileStore, FileType,
};
use futures::{StreamExt, TryFutureExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        VerifiedUniqueConnectionsIngestReportStatus, VerifiedUniqueConnectionsIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

use super::db;

pub struct UniqueConnectionsIngestor<AV> {
    pool: PgPool,
    unique_connections_receiver: Receiver<FileInfoStream<UniqueConnectionsIngestReport>>,
    verified_unique_connections_sink: FileSinkClient<VerifiedUniqueConnectionsIngestReportV1>,
    authorization_verifier: AV,
}

impl<AV> ManagedTask for UniqueConnectionsIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<AV> UniqueConnectionsIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: AV,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_unique_connections, verified_unique_conections_server) =
            VerifiedUniqueConnectionsIngestReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (unique_connections_ingest, unique_connections_server) =
            file_source::Continuous::msg_source::<UniqueConnectionsIngestReport, _>()
                .state(pool.clone())
                .store(file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::UniqueConnectionsReport.to_string())
                .create()
                .await?;

        let radio_threshold_ingestor = Self::new(
            pool.clone(),
            unique_connections_ingest,
            verified_unique_connections,
            authorization_verifier,
        );

        Ok(TaskManager::builder()
            .add_task(verified_unique_conections_server)
            .add_task(radio_threshold_ingestor)
            .add_task(unique_connections_server)
            .build())
    }

    pub fn new(
        pool: PgPool,
        unique_connections_receiver: Receiver<FileInfoStream<UniqueConnectionsIngestReport>>,
        verified_unique_connections_sink: FileSinkClient<VerifiedUniqueConnectionsIngestReportV1>,
        authorization_verifier: AV,
    ) -> Self {
        Self {
            pool,
            unique_connections_receiver,
            verified_unique_connections_sink,
            authorization_verifier,
        }
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting unique connections ingestor");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                Some(file) = self.unique_connections_receiver.recv() => {
                    self.process_unique_connections_file(file).await?;
                }
            }
        }
        tracing::info!("stopping unique connections ingestor");
        Ok(())
    }

    async fn process_unique_connections_file(
        &self,
        file_info_stream: FileInfoStream<UniqueConnectionsIngestReport>,
    ) -> anyhow::Result<()> {
        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut verified = vec![];

        while let Some(unique_connections_report) = stream.next().await {
            let verified_report_status = self
                .verify_unique_connection_report(&unique_connections_report.report)
                .await;

            if matches!(
                verified_report_status,
                VerifiedUniqueConnectionsIngestReportStatus::Valid
            ) {
                verified.push(unique_connections_report.clone());
            }

            let verified_report_proto = VerifiedUniqueConnectionsIngestReport {
                timestamp: Utc::now(),
                report: unique_connections_report,
                status: verified_report_status,
            };

            self.verified_unique_connections_sink
                .write(
                    verified_report_proto.into(),
                    &[("report_status", verified_report_status.as_str_name())],
                )
                .await?;
        }

        db::save(&mut txn, &verified).await?;
        txn.commit().await?;
        self.verified_unique_connections_sink.commit().await?;

        Ok(())
    }

    async fn verify_unique_connection_report(
        &self,
        report: &UniqueConnectionReq,
    ) -> VerifiedUniqueConnectionsIngestReportStatus {
        if !self.verify_known_carrier_key(&report.carrier_key).await {
            return VerifiedUniqueConnectionsIngestReportStatus::InvalidCarrierKey;
        }
        VerifiedUniqueConnectionsIngestReportStatus::Valid
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        self.authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
            .unwrap_or_default()
    }
}
