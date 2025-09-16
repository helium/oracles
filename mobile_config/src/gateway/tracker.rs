use crate::gateway::{db::Gateway, metadata_db::MobileHotspotInfo};
use futures::TryFutureExt;
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
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
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

    let mut stream = MobileHotspotInfo::stream(metadata);
    let mut batch: Vec<Gateway> = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0u64;

    while let Some(mhi) = stream.try_next().await? {
        if let Some(gateway) = mhi.to_gateway()? {
            batch.push(gateway);
            if batch.len() >= BATCH_SIZE {
                total += Gateway::insert_bulk(pool, &batch).await?;
                batch.clear();
            }
        }
    }

    if !batch.is_empty() {
        total += Gateway::insert_bulk(pool, &batch).await?;
    }

    let elapsed = start.elapsed();
    tracing::info!(?elapsed, affected = total, "done execute");
    metrics::histogram!(EXECUTE_DURATION_METRIC).record(start.elapsed());
    Ok(())
}
