//! Periodic job that refreshes the local `deployment_info` cache from the
//! on-chain metadata DB.
//!
//! The dbt chain tables supply every gateway field except WiFi
//! `(antenna, elevation)`, which only exists in
//! `mobile_hotspot_infos.deployment_info`. Rather than hit the metadata DB on
//! every request, this tracker bulk-copies that pair into the local
//! `deployment_info` table on an interval; gateway reads join against it.

use crate::gateway::metadata_db::{self, DeploymentInfoRecord};
use sqlx::{PgPool, Pool, Postgres, QueryBuilder};
use std::time::{Duration, Instant};
use task_manager::Periodic;

const EXECUTE_DURATION_METRIC: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "-",
    "deployment-info-tracker-execute-duration"
);

const UPSERT_CHUNK: usize = 5000;

pub struct DeploymentInfoTracker {
    pool: PgPool,
    metadata: Pool<Postgres>,
    interval: Duration,
}

impl Periodic for DeploymentInfoTracker {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        if let Err(err) = execute(&self.pool, &self.metadata).await {
            tracing::error!(?err, "error refreshing deployment_info");
        }
        Ok(())
    }
}

impl DeploymentInfoTracker {
    pub fn new(pool: PgPool, metadata: Pool<Postgres>, interval: Duration) -> Self {
        Self {
            pool,
            metadata,
            interval,
        }
    }
}

pub async fn execute(pool: &PgPool, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("starting deployment_info refresh");
    let start = Instant::now();

    let records = metadata_db::fetch_all_deployment_info(metadata).await?;
    let mut upserted = 0u64;
    for chunk in records.chunks(UPSERT_CHUNK) {
        upserted += upsert_chunk(pool, chunk).await?;
    }

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, upserted, "deployment_info refresh complete");
    metrics::histogram!(EXECUTE_DURATION_METRIC).record(elapsed);
    Ok(())
}

async fn upsert_chunk(pool: &PgPool, chunk: &[DeploymentInfoRecord]) -> anyhow::Result<u64> {
    if chunk.is_empty() {
        return Ok(0);
    }

    let mut qb = QueryBuilder::<Postgres>::new(
        "INSERT INTO deployment_info (address, antenna, elevation, refreshed_at) ",
    );

    qb.push_values(chunk, |mut b, rec| {
        b.push_bind(rec.address.to_string())
            .push_bind(rec.antenna as i32)
            .push_bind(rec.elevation as i32)
            .push("now()");
    });

    qb.push(
        r#"
        ON CONFLICT (address) DO UPDATE SET
            antenna = EXCLUDED.antenna,
            elevation = EXCLUDED.elevation,
            refreshed_at = now()
        "#,
    );

    let res = qb.build().execute(pool).await?;
    Ok(res.rows_affected())
}
