use poclora_verifier::{loader, mk_db_pool, runner, Result};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| {
                "poclora_report=info,poc_store=info,poclora_verifier=info".into()
            }),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create database pool and run migrations
    let pool = mk_db_pool(2).await?;
    tracing::info!("running migrations");
    sqlx::migrate!().run(&pool).await?;

    // configure shutdown trigger
    let (shutdown_trigger, shutdown) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    let loader = loader::Loader::from_env().await?;
    let runner = runner::Runner::from_env().await?;
    tokio::try_join!(runner.run(&shutdown), loader.run(&shutdown)).map(|_| ())
}
