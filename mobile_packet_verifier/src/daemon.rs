use crate::{burner::Burner, settings::Settings};
use file_store::{
    file_info_poller::FileInfoStream, mobile_session::DataTransferSessionIngestReport,
};
use sqlx::{Pool, Postgres};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

pub struct Daemon {
    pool: Pool<Postgres>,
    burner: Burner,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    burn_period: Duration,
}

impl Daemon {
    pub fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        burner: Burner,
    ) -> Self {
        Self {
            pool,
            burner,
            reports,
            burn_period: Duration::from_secs(60 * 60 * settings.burn_period as u64),
        }
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        let mut burn_time = Instant::now() + self.burn_period;
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
                    crate::accumulate::accumulate_sessions(&mut transaction, ts, reports).await?;
                    transaction.commit().await?;
                },
                _ = sleep_until(burn_time) => {
                    // It's time to burn
                    self.burner.burn(&self.pool).await?;
                    burn_time = Instant::now() + self.burn_period;
                }
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }
}
