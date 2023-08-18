use std::time::Duration;

use crate::{Error, Result};

const DURATION: Duration = Duration::from_secs(300);

pub async fn start(
    app_name: &str,
    pool: sqlx::Pool<sqlx::Postgres>,
    shutdown: triggered::Listener,
) -> Result<futures::future::BoxFuture<'static, Result>> {
    let pool_size_name = format!("{app_name}_db_pool_size");
    let pool_idle_name = format!("{app_name}_db_pool_idle");
    let join_handle =
        tokio::spawn(async move { run(pool_size_name, pool_idle_name, pool, shutdown).await });

    Ok(Box::pin(async move {
        match join_handle.await {
            Ok(()) => Ok(()),
            Err(err) => Err(Error::from(err)),
        }
    }))
}

pub async fn start_tm(app_name: &str, pool: sqlx::Pool<sqlx::Postgres>) {
    let pool_size_name = format!("{app_name}_db_pool_size");
    let pool_idle_name = format!("{app_name}_db_pool_idle");
    tokio::spawn(async move { run_tm(pool_size_name, pool_idle_name, pool).await });
}

async fn run(
    size_name: String,
    idle_name: String,
    pool: sqlx::Pool<sqlx::Postgres>,
    shutdown: triggered::Listener,
) {
    let mut trigger = tokio::time::interval(DURATION);

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => {
                tracing::info!("db_store: MetricTracker shutting down");
                break;
            }
            _ = trigger.tick() => {
               metrics::gauge!(size_name.clone(), pool.size() as f64);
               metrics::gauge!(idle_name.clone(), pool.num_idle() as f64);
            }
        }
    }
}

async fn run_tm(size_name: String, idle_name: String, pool: sqlx::Pool<sqlx::Postgres>) {
    let mut trigger = tokio::time::interval(DURATION);

    loop {
        trigger.tick().await;

        metrics::gauge!(size_name.clone(), pool.size() as f64);
        metrics::gauge!(idle_name.clone(), pool.num_idle() as f64);
    }
}
