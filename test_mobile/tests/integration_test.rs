use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

mod docker;

#[tokio::test]
async fn main() -> Result<()> {
    match docker::up().await {
        Ok(_) => {}
        Err(e) => panic!("{:?}", e),
    }

    sleep(Duration::from_secs(10)).await;

    docker::down().await?;

    Ok(())
}
