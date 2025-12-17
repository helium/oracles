use crate::gateway::{db::Gateway, metadata_db::MobileHotspotInfo};
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

        if let Err(e) = backfill_gateway_owners(&self.pool, &self.metadata).await {
            tracing::error!("backfill_gateway_owners is failed. {e}");
        }

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

/// Post-migration function to backfill owner data for gateways with NULL owner.
/// Gets gateways where owner is NULL, streams mobile hotspot info,
/// and updates gateways.owner with owner_changed_at = last_changed_at
pub async fn backfill_gateway_owners(
    pool: &Pool<Postgres>,
    metadata: &Pool<Postgres>,
) -> anyhow::Result<()> {
    tracing::info!("starting owner backfill");
    let start = Instant::now();

    const BATCH_SIZE: usize = 1_000;

    // Get gateways where owner is NULL
    let null_owner_gateways = Gateway::get_null_owners(pool).await?;
    let null_owner_addresses: HashMap<String, Gateway> = null_owner_gateways
        .into_iter()
        .map(|gw| (gw.address.to_string(), gw))
        .collect();

    tracing::info!(
        "found {} gateways with null owners",
        null_owner_addresses.len()
    );
    if null_owner_addresses.is_empty() {
        return Ok(());
    }

    // Stream mobile hotspot info and update matching gateways
    let total: u64 = MobileHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_filter_map(|mhi| async move {
            match mhi.to_gateway() {
                Ok(Some(gw)) => Ok(Some(gw)),
                Ok(None) => Ok(None),
                Err(e) => {
                    tracing::error!(?e, "error converting gateway");
                    Err(e)
                }
            }
        })
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_gateways, err)| err)
        .try_fold(0, |total, batch| {
            let null_owner_addresses = &null_owner_addresses;
            async move {
                let mut to_update = Vec::with_capacity(BATCH_SIZE);

                for gw in batch {
                    if let Some(existing_gw) = null_owner_addresses.get(&gw.address.to_string()) {
                        if let Some(owner) = gw.owner {
                            // Update gateway with owner from mobile hotspot info
                            let mut updated_gw = existing_gw.clone();
                            updated_gw.owner = Some(owner);
                            // Set owner_changed_at = last_changed_at
                            updated_gw.owner_changed_at = Some(existing_gw.last_changed_at);
                            to_update.push(updated_gw);
                        }
                    }
                }

                let affected = Gateway::update_owners(pool, &to_update).await?;
                Ok(total + affected)
            }
        })
        .await?;

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, updated = total, "done owner backfill");

    Ok(())
}

pub async fn execute(pool: &Pool<Postgres>, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("starting execute");
    let start = Instant::now();

    const BATCH_SIZE: usize = 1_000;

    let total: u64 = MobileHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_filter_map(|mhi| async move {
            match mhi.to_gateway() {
                Ok(Some(gw)) => Ok(Some(gw)),
                Ok(None) => Ok(None),
                Err(e) => {
                    tracing::error!(?e, "error converting gateway");
                    Err(e)
                }
            }
        })
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_gateways, err)| err)
        .try_fold(0, |total, batch| async move {
            let addresses: Vec<_> = batch.iter().map(|gw| gw.address.clone()).collect();
            let existing_gateways = Gateway::get_by_addresses(pool, addresses).await?;
            let mut existing_map = existing_gateways
                .into_iter()
                .map(|gw| (gw.address.clone(), gw))
                .collect::<HashMap<_, _>>();

            let mut to_insert = Vec::with_capacity(batch.len());

            for mut gw in batch {
                match existing_map.remove(&gw.address) {
                    None => {
                        // New gateway
                        to_insert.push(gw);
                    }
                    Some(last_gw) => {
                        let loc_changed = gw.location != last_gw.location;
                        // FYI hash includes location
                        // owner (at this moment) is not included in hash
                        let hash_changed = gw.hash != last_gw.hash;

                        let owner_changed = if gw.owner.is_none() {
                            false
                        } else {
                            gw.owner != last_gw.owner
                        };

                        // TODO at the second stage of implementing owner and owner_changed at
                        // last_changed_at should also be affected if owner was changed
                        gw.last_changed_at = if hash_changed {
                            gw.refreshed_at
                        } else {
                            last_gw.last_changed_at
                        };

                        gw.location_changed_at = if loc_changed {
                            Some(gw.refreshed_at)
                        } else {
                            last_gw.location_changed_at
                        };

                        gw.owner_changed_at = if owner_changed {
                            Some(gw.refreshed_at)
                        } else {
                            last_gw.owner_changed_at
                        };

                        // We only add record if something changed
                        // FYI hash includes location
                        if hash_changed || owner_changed {
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
