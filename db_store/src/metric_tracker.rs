use std::time::Duration;

const DURATION: Duration = Duration::from_secs(300);

pub async fn start(app_name: &str, pool: sqlx::Pool<sqlx::Postgres>) {
    let pool_size_name = format!("{app_name}_db_pool_size");
    let pool_idle_name = format!("{app_name}_db_pool_idle");
    tokio::spawn(async move { run(pool_size_name, pool_idle_name, pool).await });
}

async fn run(size_name: String, idle_name: String, pool: sqlx::Pool<sqlx::Postgres>) {
    let mut trigger = tokio::time::interval(DURATION);

    loop {
        trigger.tick().await;

        metrics::gauge!(size_name.clone()).set(pool.size() as f64);
        metrics::gauge!(idle_name.clone()).set(pool.num_idle() as f64);
    }
}
