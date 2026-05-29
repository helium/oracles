//! Reconciliation pass that fixes `antenna`/`elevation` drift in the
//! `gateways` table from `mobile_hotspot_infos.deployment_info`.
//!
//! Everything else on a gateway row comes from the
//! `chain_rewardable_entities` S3 change streams. The stream daemon does a
//! best-effort `(antenna, elevation)` lookup at event time
//! ([`crate::gateway::hotspot_change_stream`]); this tracker is the daily
//! safety net for the case where the metadata DB hadn't caught up yet —
//! and the only metadata-DB consumer that walks the full gateways table.

use crate::gateway::{db::Gateway, metadata_db};
use chrono::Utc;
use futures::stream::TryChunksError;
use futures_util::TryStreamExt;
use sqlx::{PgPool, Pool, Postgres};
use std::time::{Duration, Instant};
use task_manager::Periodic;

const EXECUTE_DURATION_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "tracker-execute-duration");

const BATCH_SIZE: usize = 500;

pub struct Tracker {
    pool: PgPool,
    metadata: Pool<Postgres>,
    interval: Duration,
}

impl Periodic for Tracker {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        if let Err(err) = execute(&self.pool, &self.metadata).await {
            tracing::error!(?err, "error reconciling antenna/elevation");
        }
        Ok(())
    }
}

impl Tracker {
    pub fn new(pool: PgPool, metadata: Pool<Postgres>, interval: Duration) -> Self {
        Self {
            pool,
            metadata,
            interval,
        }
    }
}

pub async fn execute(pool: &PgPool, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("starting antenna/elevation reconciliation");
    let start = Instant::now();

    let updated = Gateway::stream_latest_per_address(pool)
        .map_err(anyhow::Error::from)
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_, err)| err)
        .try_fold(0u64, |total, batch| async move {
            Ok(total + reconcile_batch(pool, metadata, batch).await?)
        })
        .await?;

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, updated, "reconciliation complete");
    metrics::histogram!(EXECUTE_DURATION_METRIC).record(elapsed);
    Ok(())
}

async fn reconcile_batch(
    pool: &PgPool,
    metadata: &Pool<Postgres>,
    batch: Vec<Gateway>,
) -> anyhow::Result<u64> {
    let addresses: Vec<_> = batch.iter().map(|g| g.address.clone()).collect();
    let lookups = metadata_db::fetch_antenna_and_elevation_batch(metadata, &addresses).await?;

    let now = Utc::now();
    let mut updated = 0u64;
    for gw in batch {
        let Some(&(meta_antenna, meta_elevation)) = lookups.get(&gw.address) else {
            continue;
        };

        // Only adopt non-null metadata values; never regress to null. This
        // avoids clobbering a value the stream daemon's per-event lookup
        // already captured if the metadata DB later loses it.
        let next_antenna = meta_antenna.or(gw.antenna);
        let next_elevation = meta_elevation.or(gw.elevation);
        if next_antenna == gw.antenna && next_elevation == gw.elevation {
            continue;
        }

        let next = Gateway {
            antenna: next_antenna,
            elevation: next_elevation,
            last_changed_at: now,
            ..gw
        };
        next.insert(pool).await?;
        updated += 1;
    }

    Ok(updated)
}
