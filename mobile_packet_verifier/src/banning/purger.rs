use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use sqlx::PgPool;
use task_manager::ManagedTask;

pub struct BanPurger {
    pool: PgPool,
    interval: Duration,
}

impl ManagedTask for BanPurger {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        self.run(shutdown).boxed_local()
    }
}

impl BanPurger {
    pub fn new(pool: PgPool, interval: Duration) -> Self {
        Self { pool, interval }
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("ban purger starting");
        let mut timer = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = timer.tick() => self.purge(Utc::now()).await?
            }
        }

        tracing::info!("ban purger stopping");

        Ok(())
    }

    async fn purge(&mut self, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        let deleted = crate::banning::db::cleanup_bans(&mut conn, timestamp).await?;
        tracing::info!(deleted, "purge expired bans");
        Ok(())
    }
}
