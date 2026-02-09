use crate::gateway::{
    db::{Gateway, HashParams},
    metadata_db::MobileHotspotInfo,
};
use futures::stream::TryChunksError;
use futures_util::TryStreamExt;
use sqlx::{Pool, Postgres};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use task_manager::ManagedTask;

const EXECUTE_DURATION_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "tracker-execute-duration");

pub struct Tracker {
    pool: Pool<Postgres>,
    metadata: Pool<Postgres>,
    interval: Duration,
}

impl ManagedTask for Tracker {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl Tracker {
    pub fn new(pool: Pool<Postgres>, metadata: Pool<Postgres>, interval: Duration) -> Self {
        Self {
            pool,
            metadata,
            interval,
        }
    }

    async fn run(self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting with interval: {:?}", self.interval);
        let mut interval = tokio::time::interval(self.interval);

        backfill_hashes(&self.pool, &self.metadata).await?;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    if let Err(err) = execute(&self.pool, &self.metadata).await {
                        tracing::error!(?err, "error in tracking changes to mobile radios");
                    }
                }
            }
        }

        tracing::info!("stopping");

        Ok(())
    }
}

// TODO remove after migration
pub async fn backfill_hashes(
    pool: &Pool<Postgres>,
    metadata: &Pool<Postgres>,
) -> anyhow::Result<()> {
    tracing::info!("starting backfill_hashes");
    let start = Instant::now();

    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                last_changed_at,
                COALESCE(hash, '') as hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            ORDER BY address, inserted_at DESC
        ) AS distinct_gateways
        WHERE hash = ''
        "#,
    )
    .fetch_one(pool)
    .await?;

    if count == 0 {
        tracing::info!("no gateways with null hash, skipping backfill");
        return Ok(());
    }

    tracing::info!(count, "gateways with null hash to backfill");

    const BATCH_SIZE: usize = 1_000;

    let total: u64 = MobileHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_, err)| err)
        .try_fold(0, |total, batch| async move {
            let addresses: Vec<_> = batch.iter().map(|mhi| &mhi.entity_key).collect();
            let null_hash_gateways =
                Gateway::get_by_addresses_with_null_hash(pool, addresses).await?;
            let null_hash_map: HashMap<_, _> = null_hash_gateways
                .iter()
                .map(|gw| (&gw.address, gw))
                .collect();

            let mut updated = 0u64;
            for mhi in &batch {
                if null_hash_map.contains_key(&mhi.entity_key) {
                    let hash = HashParams::from_hotspot_info(mhi).compute_hash();
                    Gateway::update_latest_hash(pool, &mhi.entity_key, &hash).await?;
                    updated += 1;
                }
            }

            Ok(total + updated)
        })
        .await?;

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, affected = total, "done backfill_hashes");

    Ok(())
}

pub async fn execute(pool: &Pool<Postgres>, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("starting execute");
    let start = Instant::now();

    const BATCH_SIZE: usize = 1_000;

    let total: u64 = MobileHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_, err)| err)
        .try_fold(0, |total, batch| async move {
            let addresses: Vec<_> = batch.iter().map(|mhi| &mhi.entity_key).collect();
            let existing_gateways = Gateway::get_by_addresses(pool, addresses).await?;
            let mut existing_map: HashMap<_, _> = existing_gateways
                .iter()
                .map(|gw| (&gw.address, gw))
                .collect();

            let mut to_insert = Vec::with_capacity(batch.len());

            for mhi in batch {
                match existing_map.remove(&mhi.entity_key) {
                    None => {
                        to_insert.push(Gateway::from_mobile_hotspot_info(&mhi));
                    }
                    Some(last_gw) => {
                        if let Some(gw) = last_gw.new_if_changed(&mhi) {
                            to_insert.push(gw);
                        }
                    }
                }
            }

            let affected = Gateway::insert_bulk(pool, &to_insert).await?;
            Ok(total + affected)
        })
        .await?;

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, affected = total, "done execute");
    metrics::histogram!(EXECUTE_DURATION_METRIC).record(elapsed);

    Ok(())
}
