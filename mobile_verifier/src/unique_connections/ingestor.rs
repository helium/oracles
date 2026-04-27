use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source,
    file_upload::FileUpload, BucketClient,
};
use file_store_oracles::{
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    unique_connections::{
        UniqueConnectionReq, UniqueConnectionsIngestReport, VerifiedUniqueConnectionsIngestReport,
    },
    FileType,
};
use futures::StreamExt;
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

use crate::{
    backfill::{Backfiller, IcebergBackfill},
    iceberg, Settings,
};

use super::db;

// ── Unique Connections backfill ───────────────────────────────────────────────

pub struct UniqueConnectionsConverter;
pub type UniqueConnectionsBackfiller = Backfiller<UniqueConnectionsConverter>;

impl IcebergBackfill for UniqueConnectionsConverter {
    type FileRecord = VerifiedUniqueConnectionsIngestReport;
    type IcebergRow = iceberg::IcebergUniqueConnections;
    const FILE_TYPE: FileType = FileType::VerifiedUniqueConnectionsReport;

    fn convert(
        record: VerifiedUniqueConnectionsIngestReport,
    ) -> Option<iceberg::IcebergUniqueConnections> {
        (record.status == VerifiedUniqueConnectionsIngestReportStatus::Valid)
            .then(|| iceberg::IcebergUniqueConnections::from(&record))
    }
}

// ── UniqueConnectionsIngestor ─────────────────────────────────────────────────

pub struct UniqueConnectionsIngestor<AV> {
    pool: PgPool,
    unique_connections_receiver: Receiver<FileInfoStream<UniqueConnectionsIngestReport>>,
    verified_unique_connections_sink: FileSinkClient<VerifiedUniqueConnectionsIngestReportV1>,
    authorization_verifier: AV,
    iceberg_writer: Option<iceberg::UniqueConnectionsWriter>,
    backfiller: UniqueConnectionsBackfiller,
}

impl<AV> ManagedTask for UniqueConnectionsIngestor<AV>
where
    AV: AuthorizationVerifier,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<AV> UniqueConnectionsIngestor<AV>
where
    AV: AuthorizationVerifier,
{
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        bucket_client: BucketClient,
        authorization_verifier: AV,
        iceberg_writer: Option<iceberg::UniqueConnectionsWriter>,
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
            file_source::continuous_source()
                .state(pool.clone())
                .bucket_client(bucket_client)
                .lookback_start_after(settings.start_after)
                .prefix(FileType::UniqueConnectionsReport.to_string())
                .create()
                .await?;

        let backfill_opts = settings
            .unique_connections_backfill
            .as_ref()
            .map(|b| b.as_options("unique-connections-backfill"));

        let (backfiller, backfill_server) = UniqueConnectionsBackfiller::create(
            pool.clone(),
            settings.buckets.output.connect().await,
            iceberg_writer.clone(),
            backfill_opts,
        )
        .await?;

        let ingestor = Self::new(
            pool.clone(),
            unique_connections_ingest,
            verified_unique_connections,
            authorization_verifier,
            iceberg_writer,
            backfiller,
        );

        Ok(TaskManager::builder()
            .add_task(verified_unique_conections_server)
            .add_task(ingestor)
            .add_task(unique_connections_server)
            .add_task(backfill_server)
            .build())
    }

    pub fn new(
        pool: PgPool,
        unique_connections_receiver: Receiver<FileInfoStream<UniqueConnectionsIngestReport>>,
        verified_unique_connections_sink: FileSinkClient<VerifiedUniqueConnectionsIngestReportV1>,
        authorization_verifier: AV,
        iceberg_writer: Option<iceberg::UniqueConnectionsWriter>,
        backfiller: UniqueConnectionsBackfiller,
    ) -> Self {
        Self {
            pool,
            unique_connections_receiver,
            verified_unique_connections_sink,
            authorization_verifier,
            iceberg_writer,
            backfiller,
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
                // Backfill runs at lowest priority — only fires when ingest has nothing ready.
                // When iceberg is not configured, recv() returns pending() immediately.
                file = self.backfiller.recv() => {
                    self.backfiller.handle(file).await?;
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
        let file_info = file_info_stream.file_info.clone();
        let write_id = file_info.key.clone();
        tracing::info!(?file_info, "processing file");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut verified = vec![];
        let mut iceberg_records = vec![];

        while let Some(unique_connections_report) = stream.next().await {
            let verified_report_status = self
                .verify_unique_connection_report(&unique_connections_report.report)
                .await;

            let is_valid = matches!(
                verified_report_status,
                VerifiedUniqueConnectionsIngestReportStatus::Valid
            );

            if is_valid {
                verified.push(unique_connections_report.clone());
            }

            let verified_report = VerifiedUniqueConnectionsIngestReport {
                timestamp: Utc::now(),
                report: unique_connections_report,
                status: verified_report_status,
            };

            if is_valid && self.iceberg_writer.is_some() {
                iceberg_records.push(iceberg::IcebergUniqueConnections::from(&verified_report));
            }

            self.verified_unique_connections_sink
                .write(
                    verified_report,
                    &[("report_status", verified_report_status.as_str_name())],
                )
                .await?;
        }

        iceberg::maybe_write_idempotent(self.iceberg_writer.as_ref(), &write_id, iceberg_records)
            .await?;

        db::save(&mut txn, &verified).await?;
        txn.commit().await?;
        self.verified_unique_connections_sink.commit().await?;
        tracing::info!(?file_info, "txn and sink committed");

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
