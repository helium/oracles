use file_store::{file_info_poller::FileInfoStream, file_sink::FileSinkClient};
use file_store_helium_proto::mobile_ban::{proto, VerifiedBanReport};
use futures::StreamExt;
use sqlx::{PgConnection, PgPool};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub type VerifiedBanReportSink = FileSinkClient<proto::VerifiedBanIngestReportV1>;
pub type VerifiedBanReportStream = FileInfoStream<VerifiedBanReport>;
pub type VerifiedBanReportSource = tokio::sync::mpsc::Receiver<VerifiedBanReportStream>;

use super::db;

pub struct BanIngestor {
    pool: PgPool,
    report_rx: Receiver<FileInfoStream<VerifiedBanReport>>,
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
    pub fn new(pool: PgPool, report_rx: VerifiedBanReportSource) -> Self {
        Self { pool, report_rx }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting ban ingestor");

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                file = self.report_rx.recv() => {
                    let Some(file_info_stream) = file else {
                        anyhow::bail!("hotspot ban FileInfoPoller sender was dropped unexpectedly");
                    };
                    self.handle_ban_report_file(file_info_stream).await?;
                }

            }
        }

        tracing::info!("stopping ban ingestor");
        Ok(())
    }

    async fn handle_ban_report_file(
        &self,
        file_info_stream: VerifiedBanReportStream,
    ) -> anyhow::Result<()> {
        let file = &file_info_stream.file_info.key;
        tracing::info!(file, "processing");

        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        while let Some(report) = stream.next().await {
            handle_verified_ban_report(&mut txn, report).await?;
        }

        txn.commit().await?;

        Ok(())
    }
}

pub async fn handle_verified_ban_report(
    conn: &mut PgConnection,
    report: VerifiedBanReport,
) -> anyhow::Result<()> {
    if report.is_valid() {
        db::update_hotspot_ban(conn, report).await?;
    }

    Ok(())
}
