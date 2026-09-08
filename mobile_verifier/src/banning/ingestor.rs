use std::ops::ControlFlow;

use chrono::Utc;
use file_store::BucketClient;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source,
    file_upload::FileUpload,
};
use file_store_oracles::mobile_ban::{
    proto::{VerifiedBanIngestReportStatus, VerifiedBanIngestReportV1},
    BanReport, VerifiedBanReport,
};
use file_store_oracles::{
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use futures::StreamExt;
use helium_proto::services::mobile_config::NetworkKeyRole;
use sqlx::PgPool;
use task_manager::{ChannelConsumer, ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    authorization::{AuthorizationVerifier, AuthorizedKeys},
    iceberg, Settings,
};

// ── BanIngestor ───────────────────────────────────────────────────────────────

pub struct BanIngestor {
    pool: PgPool,
    authorized_keys: AuthorizedKeys,
    report_rx: Receiver<FileInfoStream<BanReport>>,
    verified_sink: FileSinkClient<VerifiedBanIngestReportV1>,
    iceberg_writer: iceberg::BanWriter,
}

impl ChannelConsumer for BanIngestor {
    type Item = FileInfoStream<BanReport>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.report_rx.recv().await
    }

    async fn handle(&mut self, file_info_stream: Self::Item) -> anyhow::Result<()> {
        self.process_file(file_info_stream).await
    }

    async fn on_receiver_closed(&mut self) -> anyhow::Result<ControlFlow<()>> {
        Err(anyhow::anyhow!(
            "hotspot ban FileInfoPoller sender was dropped unexpectedly"
        ))
    }
}

impl BanIngestor {
    pub async fn create_managed_task(
        pool: PgPool,
        file_upload: FileUpload,
        bucket_client: BucketClient,
        authorized_keys: AuthorizedKeys,
        settings: &Settings,
        iceberg_writer: iceberg::BanWriter,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_sink, verified_sink_server) = VerifiedBanIngestReportV1::file_sink(
            settings.store_base_path(),
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (report_rx, ingest_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .lookback_start_after(settings.start_after)
            .prefix(FileType::MobileBanReport.to_string())
            .create()
            .await?;

        let ingestor = Self::new(
            pool,
            authorized_keys,
            report_rx,
            verified_sink,
            iceberg_writer,
        );

        Ok(TaskManager::builder()
            .add_task(verified_sink_server)
            .add_task(ingest_server)
            .add_task(task_manager::channel_consumer(ingestor))
            .build())
    }

    pub fn new(
        pool: PgPool,
        authorized_keys: AuthorizedKeys,
        report_rx: Receiver<FileInfoStream<BanReport>>,
        verified_sink: FileSinkClient<VerifiedBanIngestReportV1>,
        iceberg_writer: iceberg::BanWriter,
    ) -> Self {
        Self {
            pool,
            authorized_keys,
            report_rx,
            verified_sink,
            iceberg_writer,
        }
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<BanReport>,
    ) -> anyhow::Result<()> {
        let write_id = file_info_stream.file_info.key.clone();
        tracing::info!(file = %write_id, "processing");

        // Bans are not stored locally; the transaction exists only so the
        // file-info poller can record this file as processed.
        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut iceberg_records = vec![];
        let mut invalid_iceberg_records = vec![];

        while let Some(report) = stream.next().await {
            let verified_report = process_ban_report(&self.authorized_keys, report);
            let record = iceberg::IcebergBan::from(&verified_report);
            if verified_report.is_valid() {
                iceberg_records.push(record);
            } else {
                invalid_iceberg_records.push(iceberg::IcebergInvalidBan::new(
                    record,
                    verified_report.status,
                ));
            }
            let status = verified_report.status.as_str_name();
            self.verified_sink
                .write(verified_report, &[("status", status)])
                .await?;
        }

        self.iceberg_writer
            .write(&write_id, iceberg_records, invalid_iceberg_records)
            .await?;

        self.verified_sink.commit().await?;
        txn.commit().await?;

        Ok(())
    }
}

/// Stamp a ban report with whether its submitter was authorized to issue bans.
///
/// The verdict is the whole product: the report is republished for
/// mobile-packet-verifier to act on, and nothing is recorded locally.
pub fn process_ban_report(
    auth_verifier: &impl AuthorizationVerifier,
    report: BanReport,
) -> VerifiedBanReport {
    let status = get_verified_status(auth_verifier, &report.report.ban_pubkey);

    VerifiedBanReport {
        verified_timestamp: Utc::now(),
        report,
        status,
    }
}

fn get_verified_status(
    auth_verifier: &impl AuthorizationVerifier,
    pubkey: &helium_crypto::PublicKeyBinary,
) -> VerifiedBanIngestReportStatus {
    match auth_verifier.is_authorized(pubkey, NetworkKeyRole::Banning) {
        true => VerifiedBanIngestReportStatus::Valid,
        false => VerifiedBanIngestReportStatus::InvalidBanKey,
    }
}
