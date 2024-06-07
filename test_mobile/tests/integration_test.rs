use std::time::Duration;

use anyhow::Result;
use common::{docker::Docker, get_rewards, hotspot::Hotspot, hours_ago, load_pcs_keypair};
use test_mobile::cli::assignment::CENTER_CELL;
use uuid::Uuid;

mod common;

struct TestGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> TestGuard<F> {
    fn new() -> Self {
        TestGuard(None)
    }

    fn set_cleanup(&mut self, f: F) {
        self.0 = Some(f);
    }
}

impl<F: FnOnce()> Drop for TestGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

#[tokio::test]
async fn main() -> Result<()> {
    let docker = Docker::new();

    // Function to execute after the test if it's successful
    let cleanup = || {
        tracing::info!("Test succeeded! Running cleanup...");
        docker.down().unwrap();
    };

    let mut guard = TestGuard::new();

    custom_tracing::init(
        "info,integration_test=debug".to_string(),
        custom_tracing::Settings::default(),
    )
    .await?;

    match docker.up() {
        Ok(_) => {
            tracing::info!("docker compose started")
        }
        Err(e) => panic!("docker::up failed: {:?}", e),
    }

    let pcs_keypair = load_pcs_keypair().await?;

    let api_token = "api-token".to_string();

    let mut hotspot1 = Hotspot::new(api_token, CENTER_CELL).await?;
    let co_uuid = Uuid::new_v4();

    hotspot1
        .submit_coverage_object(co_uuid, pcs_keypair.clone())
        .await?;

    hotspot1
        .submit_speedtest(hours_ago(2), 500_000_000, 500_000_000, 25)
        .await?;
    hotspot1
        .submit_speedtest(hours_ago(1), 500_000_000, 500_000_000, 25)
        .await?;

    // giving time for submit_coverage_object FIXME
    let _ = tokio::time::sleep(Duration::from_secs(60)).await;

    for x in (1..=24).rev() {
        hotspot1
            .submit_wifi_heartbeat(hours_ago(x), co_uuid)
            .await?;
    }

    let mut retry = 0;
    const MAX_RETRIES: u32 = 6 * 5;
    const RETRY_WAIT: Duration = Duration::from_secs(10);
    while retry <= MAX_RETRIES {
        match get_rewards(hotspot1.b58().to_string()).await {
            Err(e) => {
                tracing::error!("failed to get rewards for {} {e:?} ", hotspot1.b58());
                retry += 1;
                tokio::time::sleep(RETRY_WAIT).await;
            }
            Ok(None) => {
                tracing::debug!("no rewards for {}", hotspot1.b58());
                retry += 1;
                tokio::time::sleep(RETRY_WAIT).await;
            }
            Ok(Some(reward)) => {
                tracing::debug!("rewards for {} are {reward}", hotspot1.b58());
                let expected = 49180327868852;
                assert_eq!(
                    expected,
                    reward,
                    "rewards for {} are wrong {reward} vs expected {expected}",
                    hotspot1.b58()
                );
                guard.set_cleanup(cleanup); // Activate the guard if the test is successful
                return Ok(());
            }
        }
    }

    Ok(())
}
