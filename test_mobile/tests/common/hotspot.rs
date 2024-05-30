use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use helium_crypto::{KeyTag, Keypair, Sign};
use helium_proto::services::poc_mobile::{Client as PocMobileClient, SpeedtestReqV1};
use prost::Message;
use rand::rngs::OsRng;
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::{metadata::MetadataValue, transport::Channel};

pub struct Hotspot {
    client: PocMobileClient<Channel>,
    keypair: Keypair,
}

impl Hotspot {
    pub async fn new() -> Self {
        let endpoint = "http://127.0.0.1:9080";

        let client = (|| PocMobileClient::connect(endpoint))
            .retry(&ExponentialBuilder::default())
            .await
            .expect("client connect");

        let keypair = Keypair::generate(KeyTag::default(), &mut OsRng);

        let b58 = keypair_to_bs58(&keypair);

        tracing::info!("hotspot {b58} connected to ingester");

        Self { client, keypair }
    }

    pub async fn submit_speedtest(mut self) -> Result<()> {
        let mut speedtest_req = SpeedtestReqV1 {
            pub_key: self.keypair.public_key().into(),
            serial: "hotspot1-serial".to_string(),
            timestamp: now() as u64,
            upload_speed: 100,
            download_speed: 100,
            latency: 25,
            signature: vec![],
        };

        speedtest_req.signature = self
            .keypair
            .sign(&speedtest_req.encode_to_vec())
            .expect("sign");

        let mut request = tonic::Request::new(speedtest_req.clone());

        let metadata_value = MetadataValue::from_str("Bearer api-token").unwrap();
        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        let _r = self.client.submit_speedtest(request).await?;

        tracing::debug!("submitted speedtest {:?}", speedtest_req);

        Ok(())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn keypair_to_bs58(keypair: &Keypair) -> String {
    bs58::encode(keypair.public_key().to_vec()).into_string()
}
