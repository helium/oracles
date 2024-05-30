use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use docker::Docker;
use helium_crypto::{KeyTag, Keypair, Sign};
use helium_proto::services::poc_mobile::{Client as PocMobileClient, SpeedtestReqV1};
use prost::Message;
use rand::rngs::OsRng;
use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::metadata::MetadataValue;

mod docker;

#[tokio::test]
async fn main() -> Result<()> {
    custom_tracing::init("info".to_string(), custom_tracing::Settings::default()).await?;

    let docker = Docker::new();

    match docker.up() {
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

    // let _ = tokio::time::sleep(Duration::from_secs(10)).await;

    let mut client = (|| PocMobileClient::connect(endpoint))
        .retry(&ExponentialBuilder::default())
        .await
        .expect("client connect");

    let hotspot1 = generate_keypair();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let mut speedtest_req = SpeedtestReqV1 {
        pub_key: hotspot1.public_key().into(),
        serial: "hotspot1-serial".to_string(),
        timestamp: now as u64,
        upload_speed: 100,
        download_speed: 100,
        latency: 25,
        signature: vec![],
    };

    speedtest_req.signature = hotspot1.sign(&speedtest_req.encode_to_vec()).expect("sign");

    let mut request = tonic::Request::new(speedtest_req);

    let metadata_value = MetadataValue::from_str("Bearer api-token").unwrap();
    request
        .metadata_mut()
        .insert("authorization", metadata_value);

    let _r = client.submit_speedtest(request).await?;

    // let _ = tokio::time::sleep(Duration::from_secs(2)).await;

    // assert_eq!(true, false);

    Ok(())
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}
