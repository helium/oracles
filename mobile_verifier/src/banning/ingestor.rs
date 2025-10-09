use chrono::Utc;
use file_store::{
    file_upload::FileUpload,
    mobile_ban::{
        self, BanReport, BanReportSource, BanReportStream, VerifiedBanIngestReportStatus,
        VerifiedBanReport, VerifiedBanReportSink,
    },
    traits::{FileSinkCommitStrategy, FileSinkRollTime},
};
use futures::StreamExt;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{authorization_client::AuthorizationVerifier, AuthorizationClient};
use sqlx::{PgConnection, PgPool};
use task_manager::{ManagedTask, TaskManager};

use crate::Settings;

use super::db;

pub struct BanIngestor {
    pool: PgPool,
    auth_verifier: AuthorizationClient,
    report_rx: BanReportSource,
    verified_sink: VerifiedBanReportSink,
}

impl ManagedTask for BanIngestor {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl BanIngestor {
    pub async fn create_managed_task(
        pool: PgPool,
        file_upload: FileUpload,
        file_store_client: file_store::Client,
        bucket: String,
        auth_verifier: AuthorizationClient,
        settings: &Settings,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_sink, verified_sink_server) = mobile_ban::verified_report_sink(
            settings.store_base_path(),
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (report_rx, ingest_server) = mobile_ban::report_source(
            pool.clone(),
            file_store_client,
            bucket,
            settings.start_after,
        )
        .await?;

        let ingestor = Self {
            pool,
            auth_verifier,
            report_rx,
            verified_sink,
        };

        Ok(TaskManager::builder()
            .add_task(verified_sink_server)
            .add_task(ingest_server)
            .add_task(ingestor)
            .build())
    }

    pub fn new(
        pool: PgPool,
        auth_verifier: AuthorizationClient,
        report_rx: BanReportSource,
        verified_sink: VerifiedBanReportSink,
    ) -> Self {
        Self {
            pool,
            auth_verifier,
            report_rx,
            verified_sink,
        }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting ban ingestor");

        loop {
            tokio::select! {
                biased;
                _= &mut shutdown => break,
                msg = self.report_rx.recv() => {
                    let Some(file_info_stream) = msg else {
                        anyhow::bail!("hotspot ban FileInfoPoller sender was dropped unexpectedly");
                    };
                    self.process_file(file_info_stream).await?;
                }
            }
        }

        tracing::info!("stopping ban ingestor");

        Ok(())
    }

    async fn process_file(&self, file_info_stream: BanReportStream) -> anyhow::Result<()> {
        let file = &file_info_stream.file_info.key;
        tracing::info!(file, "processing");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        while let Some(report) = stream.next().await {
            let verified_report = process_ban_report(&mut txn, &self.auth_verifier, report).await?;
            let status = verified_report.status.as_str_name();
            self.verified_sink
                .write(verified_report, &[("status", status)])
                .await?;
        }

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
