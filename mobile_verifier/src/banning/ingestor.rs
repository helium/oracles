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
use mobile_config::client::{authorization_client::AuthorizationVerifier, AuthorizationClient};
use sqlx::{PgConnection, PgPool};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    backfill::{Backfiller, IcebergBackfill},
    iceberg, Settings,
};

use super::db;

// ── Ban backfill ──────────────────────────────────────────────────────────────

pub struct BanConverter;
pub type BanBackfiller = Backfiller<BanConverter>;

impl IcebergBackfill for BanConverter {
    type FileRecord = VerifiedBanReport;
    type IcebergRow = iceberg::IcebergBan;
    const FILE_TYPE: FileType = FileType::VerifiedMobileBanReport;

    fn convert(record: VerifiedBanReport) -> Option<iceberg::IcebergBan> {
        record
            .is_valid()
            .then(|| iceberg::IcebergBan::from(&record))
    }
}

// ── BanIngestor ───────────────────────────────────────────────────────────────

pub struct BanIngestor {
    pool: PgPool,
    auth_verifier: AuthorizationClient,
    report_rx: Receiver<FileInfoStream<BanReport>>,
    verified_sink: FileSinkClient<VerifiedBanIngestReportV1>,
    iceberg_writer: Option<iceberg::BanWriter>,
    backfiller: BanBackfiller,
}

impl ManagedTask for BanIngestor {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl BanIngestor {
    pub async fn create_managed_task(
        pool: PgPool,
        file_upload: FileUpload,
        bucket_client: BucketClient,
        auth_verifier: AuthorizationClient,
        settings: &Settings,
        iceberg_writer: Option<iceberg::BanWriter>,
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

        let backfill_opts = settings
            .ban_backfill
            .as_ref()
            .map(|b| b.as_options("ban-backfill"));

        let (backfiller, backfill_server) = BanBackfiller::create(
            pool.clone(),
            settings.buckets.output.connect().await,
            iceberg_writer.clone(),
            backfill_opts,
        )
        .await?;

        let ingestor = Self::new(
            pool,
            auth_verifier,
            report_rx,
            verified_sink,
            iceberg_writer,
            backfiller,
        );

        Ok(TaskManager::builder()
            .add_task(verified_sink_server)
            .add_task(ingest_server)
            .add_task(backfill_server)
            .add_task(ingestor)
            .build())
    }

    pub fn new(
        pool: PgPool,
        auth_verifier: AuthorizationClient,
        report_rx: Receiver<FileInfoStream<BanReport>>,
        verified_sink: FileSinkClient<VerifiedBanIngestReportV1>,
        iceberg_writer: Option<iceberg::BanWriter>,
        backfiller: BanBackfiller,
    ) -> Self {
        Self {
            pool,
            auth_verifier,
            report_rx,
            verified_sink,
            iceberg_writer,
            backfiller,
        }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting ban ingestor");

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                msg = self.report_rx.recv() => {
                    let Some(file_info_stream) = msg else {
                        anyhow::bail!("hotspot ban FileInfoPoller sender was dropped unexpectedly");
                    };
                    self.process_file(file_info_stream).await?;
                }
                // Backfill runs at lowest priority — only fires when ingest has nothing ready.
                // When iceberg is not configured, recv() returns pending() immediately.
                file = self.backfiller.recv() => {
                    self.backfiller.handle(file).await?;
                }
            }
        }

        tracing::info!("stopping ban ingestor");

        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<BanReport>,
    ) -> anyhow::Result<()> {
        let write_id = file_info_stream.file_info.key.clone();
        tracing::info!(file = %write_id, "processing");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut iceberg_records = vec![];

        while let Some(report) = stream.next().await {
            let verified_report = process_ban_report(&mut txn, &self.auth_verifier, report).await?;
            if verified_report.is_valid() && self.iceberg_writer.is_some() {
                iceberg_records.push(iceberg::IcebergBan::from(&verified_report));
            }
            let status = verified_report.status.as_str_name();
            self.verified_sink
                .write(verified_report, &[("status", status)])
                .await?;
        }

        iceberg::maybe_write_idempotent(self.iceberg_writer.as_ref(), &write_id, iceberg_records)
            .await?;

        self.verified_sink.commit().await?;
        txn.commit().await?;

        Ok(())
    }
}

pub async fn process_ban_report(
    conn: &mut PgConnection,
    auth_verifier: &impl AuthorizationVerifier,
    report: BanReport,
) -> anyhow::Result<VerifiedBanReport> {
    let status = get_verified_status(auth_verifier, &report.report.ban_pubkey).await?;

    let verified_report = VerifiedBanReport {
        verified_timestamp: Utc::now(),
        report,
        status,
    };

    if verified_report.is_valid() {
        db::update_hotspot_ban(conn, &verified_report).await?;
    }
    Ok(verified_report)
}

async fn get_verified_status(
    auth_verifier: &impl AuthorizationVerifier,
    pubkey: &helium_crypto::PublicKeyBinary,
) -> anyhow::Result<VerifiedBanIngestReportStatus> {
    let is_authorized = auth_verifier
        .verify_authorized_key(pubkey, NetworkKeyRole::Banning)
        .await?;
    let status = match is_authorized {
        true => VerifiedBanIngestReportStatus::Valid,
        false => VerifiedBanIngestReportStatus::InvalidBanKey,
    };
    Ok(status)
}
