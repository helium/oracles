use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use task_manager::Periodic;

pub struct BanPurger {
    pool: PgPool,
    interval: Duration,
}

impl Periodic for BanPurger {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        self.purge(Utc::now()).await
    }
}

impl BanPurger {
    pub fn new(pool: PgPool, interval: Duration) -> Self {
        Self { pool, interval }
    }

    async fn purge(&mut self, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        let deleted = crate::banning::db::cleanup_bans(&mut conn, timestamp).await?;
        tracing::info!(deleted, "purge expired bans");
        Ok(())
    }
}
