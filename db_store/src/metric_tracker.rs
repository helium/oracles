use std::time::Duration;

use crate::{Error, Result};

const DURATION: Duration = Duration::from_secs(300);

pub async fn start(
    app_name: String,
    pool: sqlx::Pool<sqlx::Postgres>,
    shutdown: triggered::Listener,
) -> Result<futures::future::BoxFuture<'static, Result>> {
    let join_handle = tokio::spawn(async move { run(app_name, pool, shutdown).await });

    Ok(Box::pin(async move {
        match join_handle.await {
            Ok(()) => Ok(()),
            Err(err) => Err(Error::from(err)),
        }
    }))
}

async fn run(app_name: String, pool: sqlx::Pool<sqlx::Postgres>, shutdown: triggered::Listener) {
    let mut trigger = tokio::time::interval(DURATION);
    let pool_size_name = format!("{app_name}_db_pool_size");
    let pool_idle_name = format!("{app_name}_db_pool_idle");

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => {
                tracing::info!("db_store: MetricTracker shutting down");
                break;
            }
            _ = trigger.tick() => {
               metrics::gauge!(pool_size_name.clone(), pool.size() as f64);
               metrics::gauge!(pool_idle_name.clone(), pool.num_idle() as f64);
            }
        }
    }
}
