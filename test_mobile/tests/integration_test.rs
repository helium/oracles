use anyhow::Result;
use std::time::Duration;

mod docker;

#[tokio::test]
async fn main() -> Result<()> {
    custom_tracing::init("info".to_string(), custom_tracing::Settings::default()).await?;

    match docker::up().await {
        Ok(_) => {
            tracing::info!("docker compose started")
        }
        Err(e) => panic!("docker::up failed: {:?}", e),
    }

    let endpoint = "http://127.0.0.1:9080";
    let max_retries = 10;
    let retry_delay = Duration::from_secs(1);

    match docker::check_ingest_up(endpoint, max_retries, retry_delay).await {
        Ok(_) => {
            tracing::info!("Ingest is UP, lets go!")
        }
        Err(e) => panic!("docker::check_ingest_up failed: {:?}", e),
    }

    docker::down().await?;

    Ok(())
}
