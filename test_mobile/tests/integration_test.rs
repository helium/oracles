use anyhow::Result;
use common::{docker::Docker, hotspot::Hotspot};
use std::time::Duration;

mod common;

#[tokio::test]
async fn main() -> Result<()> {
    custom_tracing::init(
        "info,integration_test=debug".to_string(),
        custom_tracing::Settings::default(),
    )
    .await?;

    if std::env::var("DOCKER").is_ok() {
        let docker = Docker::new();

        match docker.up() {
            Ok(_) => {
                tracing::info!("docker compose started")
            }
            Err(e) => panic!("docker::up failed: {:?}", e),
        }
    }

    let mut hotspot1 = Hotspot::new("api-token".to_string()).await;

    hotspot1.submit_speedtest(1001, 1001, 25).await?;
    hotspot1.submit_speedtest(1002, 1002, 25).await?;

    hotspot1.submit_coverage_object().await?;

    let _ = tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
