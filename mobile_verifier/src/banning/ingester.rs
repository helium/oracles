use chrono::Utc;
use file_store::mobile_ban::{
    BanReport, BanReportSource, BanReportStream, VerifiedBanIngestReportStatus, VerifiedBanReport,
    VerifiedBanReportSink,
};
use futures::{StreamExt, TryFutureExt};
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{authorization_client::AuthorizationVerifier, AuthorizationClient};
use sqlx::{PgConnection, PgPool};
use task_manager::ManagedTask;

use super::db;

pub struct BanIngester {
    pool: PgPool,
    auth_verifier: AuthorizationClient,
    report_rx: BanReportSource,
    verified_sink: VerifiedBanReportSink,
}

impl ManagedTask for BanIngester {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl BanIngester {
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
        tracing::info!(" ban ingestor starting");

        loop {
            tokio::select! {
                _= &mut shutdown => break,
                msg = self.report_rx.recv() => {
                    let Some(file_info_stream) = msg else {
                        tracing::warn!(" ban report rx dropped");
                        break;
                    };
                    self.process_file(file_info_stream).await?;
                }
            }
        }

        tracing::info!(" ban ingestor stopping");

        Ok(())
    }

    async fn process_file(&self, file_info_stream: BanReportStream) -> anyhow::Result<()> {
        let file = &file_info_stream.file_info.key;
        tracing::info!(file, "processing");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        while let Some(report) = stream.next().await {
            let verified_report = process_ban_report(&mut txn, &self.auth_verifier, report).await?;
            self.verified_sink.write(verified_report, &[]).await?;
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
    let status = get_verified_status(auth_verifier, &report.report.ban_key).await?;

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
