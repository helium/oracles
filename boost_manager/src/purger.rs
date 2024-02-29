use crate::db;
use chrono::Duration as ChronoDuration;
use futures::{future::LocalBoxFuture, TryFutureExt};
use sqlx::{Pool, Postgres};
use std::time::Duration;
use task_manager::ManagedTask;

const PURGE_INTERVAL: Duration = Duration::from_secs(30);

pub struct Purger {
    pool: Pool<Postgres>,
    retention_period: ChronoDuration,
}

impl ManagedTask for Purger {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl Purger {
    pub fn new(pool: Pool<Postgres>, retention_period: ChronoDuration) -> Self {
        Self {
            pool,
            retention_period,
        }
    }

    async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting Purger");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = tokio::time::sleep(PURGE_INTERVAL) => {
                    purge(&self.pool, self.retention_period).await?;
                }
            }
        }
        tracing::info!("stopping Purger");
        Ok(())
    }
}

pub async fn purge(pool: &Pool<Postgres>, retention_period: ChronoDuration) -> anyhow::Result<()> {
    let num_records_purged = db::purge_stale_records(pool, retention_period).await?;
    tracing::info!("purged {} stale records", num_records_purged);
    Ok(())
}
