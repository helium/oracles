use crate::gateway::{db::Gateway, metadata_db::IOTHotspotInfo};
use futures::stream::TryChunksError;
use futures_util::TryStreamExt;
use sqlx::{Pool, Postgres};
use std::time::{Duration, Instant};
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

    let total: u64 = IOTHotspotInfo::stream(metadata)
        .map_err(anyhow::Error::from)
        .try_filter_map(|mhi| async move { mhi.to_gateway() })
        .try_chunks(BATCH_SIZE)
        .map_err(|TryChunksError(_gateways, err)| err)
        .try_fold(0, |total, batch| async move {
            let affected = Gateway::insert_bulk(pool, &batch).await?;
            Ok(total + affected)
        })
        .await?;

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, affected = total, "done execute");
    metrics::histogram!(EXECUTE_DURATION_METRIC).record(elapsed);

    Ok(())
}
