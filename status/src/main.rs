use poc5g_status::{loader, mk_db_pool, server, Result};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "poc5g_status=debug,poc_store=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    poc_common::install_metrics();

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

    let server = server::Server::from_env().await?;
    let loader = loader::Loader::from_env().await?;

    tracing::info!("starting");
    tokio::try_join!(server.run(&shutdown), loader.run(&shutdown)).map(|_| ())
}
