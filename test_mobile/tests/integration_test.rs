use anyhow::Result;
use common::{docker::Docker, hotspot::Hotspot, hours_ago};
use test_mobile::cli::assignment::CENTER_CELL;
use uuid::Uuid;

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

    let api_token = "api-token".to_string();

    let mut hotspot1 = Hotspot::new(api_token, CENTER_CELL).await?;
    let co_uuid = Uuid::new_v4();

    hotspot1.submit_coverage_object(co_uuid).await?;

    hotspot1
        .submit_speedtest(500_000_000, 500_000_000, 25)
        .await?;
    hotspot1
        .submit_speedtest(500_000_000, 500_000_000, 25)
        .await?;

    for x in (1..=12).rev() {
        hotspot1
            .submit_wifi_heartbeat(hours_ago(x), co_uuid)
            .await?;
    }

    Ok(())
}
