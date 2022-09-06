use mobile_verifier::{Result, Server};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "mobile_rewards=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    Server::new().await?.run(shutdown_listener).await?;

    Ok(())
}
