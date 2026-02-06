use crate::gateway::{
    db::{Gateway, HashParams},
    metadata_db::MobileHotspotInfo,
};
use chrono::Utc;
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

pub async fn execute(pool: &Pool<Postgres>, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("starting execute");
    let start = Instant::now();

    const BATCH_SIZE: usize = 1_000;

    let total: u64 = MobileHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_gateways, err)| err)
        .try_fold(0, |total, batch| async move {
            let addresses: Vec<_> = batch.iter().map(|mhi| mhi.entity_key.clone()).collect();
            let existing_gateways = Gateway::get_by_addresses(pool, addresses).await?;
            let mut existing_map = existing_gateways
                .into_iter()
                .map(|gw| (gw.address.clone(), gw))
                .collect::<HashMap<_, _>>();

            let mut to_insert = Vec::with_capacity(batch.len());

            for mhi in batch {
                let refreshed_at = mhi.refreshed_at.unwrap_or_else(Utc::now);

                let hash_params = HashParams::from_hotspot_info(&mhi);
                let new_hash = hash_params.compute_hash();
                match existing_map.remove(&mhi.entity_key) {
                    None => {
                        let gw = Gateway {
                            address: mhi.entity_key.clone(),
                            created_at: mhi.created_at,
                            last_changed_at: refreshed_at,
                            hash: new_hash,
                            // if location not none, then changed = refreshed_at
                            location_changed_at: mhi.location.map(|_| refreshed_at),
                            owner_changed_at: Some(refreshed_at),
                            hash_params,
                        };
                        to_insert.push(gw);
                    }
                    Some(last_gw) => {
                        if last_gw.hash == new_hash {
                            // nothing changed
                            continue;
                        }
                        let loc_changed = mhi.location != last_gw.location().map(|v| v as i64);

                        let owner_changed = if mhi.owner.is_none() {
                            false
                        } else {
                            mhi.owner.as_deref() != last_gw.owner()
                        };

                        let last_changed_at = refreshed_at;

                        let location_changed_at = if loc_changed {
                            Some(refreshed_at)
                        } else {
                            last_gw.location_changed_at
                        };

                        let owner_changed_at = if owner_changed {
                            Some(refreshed_at)
                        } else {
                            last_gw.owner_changed_at
                        };

                        let gw = Gateway {
                            address: mhi.entity_key.clone(),
                            created_at: mhi.created_at,
                            last_changed_at,
                            hash: new_hash,
                            location_changed_at,
                            owner_changed_at,
                            hash_params,
                        };

                        to_insert.push(gw);
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
