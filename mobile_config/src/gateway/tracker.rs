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
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
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
                        let hash_changed = gw.hash != last_gw.hash;

                        // FYI hash includes location
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

                        // We only add record if something changed
                        // FYI hash includes location
                        if hash_changed {
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
