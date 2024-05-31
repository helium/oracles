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
    serial: String,
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

        Self {
            client,
            keypair,
            serial: b58,
        }
    }

    pub async fn submit_speedtest(
        &mut self,
        upload_speed: u64,
        download_speed: u64,
        latency: u32,
    ) -> Result<()> {
        let mut speedtest_req = SpeedtestReqV1 {
            pub_key: self.keypair.public_key().into(),
            serial: self.serial.clone(),
            timestamp: now() as u64,
            upload_speed,
            download_speed,
            latency,
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

        let res = self.client.submit_speedtest(request).await?;

        tracing::debug!("submitted speedtest {:?}, {:?}", speedtest_req, res);

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
