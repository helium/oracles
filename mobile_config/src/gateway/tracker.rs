use crate::gateway::{self, metadata_db::MobileHotspotInfo};
use futures::TryFutureExt;
use futures_util::TryStreamExt;
use solana::helium_lib::anchor_spl::token::spl_token::error;
use sqlx::{Pool, Postgres};
use std::time::Duration;
use task_manager::ManagedTask;

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
    let mut stream = MobileHotspotInfo::stream(metadata);

    while let Some(mhi) = stream.try_next().await? {
        if let Some(gateway) = mhi.to_gateway()? {
            tracing::debug!(?gateway, "inserting gateway from mobile hotspot info");
            gateway.insert(&pool).await?;
        }
    }

    Ok(())
}
