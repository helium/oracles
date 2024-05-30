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

    let docker = Docker::new();

    match docker.up() {
        Ok(_) => {
            tracing::info!("docker compose started")
        }
        Err(e) => panic!("docker::up failed: {:?}", e),
    }

    let hotspot1 = Hotspot::new().await;
    hotspot1.submit_speedtest().await?;

    let _ = tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
