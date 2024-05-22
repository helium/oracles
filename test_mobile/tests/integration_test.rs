use anyhow::Result;
use helium_crypto::{ed25519::Keypair, Network::MainNet};
use rand::rngs::OsRng;
use std::time::Duration;
use tokio::time::sleep;

mod docker;

#[tokio::test]
async fn main() -> Result<()> {
    let keypair = Keypair::generate(MainNet, &mut OsRng);

    match docker::up().await {
        Ok(_) => {}
        Err(e) => panic!("{:?}", e),
    }

    sleep(Duration::from_secs(10)).await;

    docker::down().await?;

    Ok(())
}
