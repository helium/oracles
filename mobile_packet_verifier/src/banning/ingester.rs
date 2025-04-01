use file_store::mobile_ban::{VerifiedBanReportSource, VerifiedBanReportStream};
use futures::{FutureExt, StreamExt};
use sqlx::PgPool;
use task_manager::ManagedTask;

pub struct BanIngestor {
    pool: PgPool,
    report_rx: VerifiedBanReportSource,
}

impl ManagedTask for BanIngestor {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        self.run(shutdown).boxed_local()
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
            crate::banning::db::update_hotspot_ban(&mut txn, report).await?;
        }

        txn.commit().await?;

        Ok(())
    }
}
