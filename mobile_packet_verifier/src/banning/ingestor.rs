use std::ops::ControlFlow;

use file_store::file_info_poller::FileInfoStream;
use file_store_oracles::mobile_ban::VerifiedBanReport;
use futures::StreamExt;
use sqlx::{PgConnection, PgPool};
use task_manager::ChannelConsumer;
use tokio::sync::mpsc::Receiver;

use super::db;

pub struct BanIngestor {
    pool: PgPool,
    report_rx: Receiver<FileInfoStream<VerifiedBanReport>>,
}

impl ChannelConsumer for BanIngestor {
    type Item = FileInfoStream<VerifiedBanReport>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.report_rx.recv().await
    }

    async fn handle(&mut self, file_info_stream: Self::Item) -> anyhow::Result<()> {
        self.handle_ban_report_file(file_info_stream).await
    }

    async fn on_receiver_closed(&mut self) -> anyhow::Result<ControlFlow<()>> {
        Err(anyhow::anyhow!(
            "hotspot ban FileInfoPoller sender was dropped unexpectedly"
        ))
    }
}

impl BanIngestor {
    pub fn new(pool: PgPool, report_rx: Receiver<FileInfoStream<VerifiedBanReport>>) -> Self {
        Self { pool, report_rx }
    }

    async fn handle_ban_report_file(
        &self,
        file_info_stream: FileInfoStream<VerifiedBanReport>,
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
