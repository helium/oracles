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
use tonic::{metadata::MetadataValue, transport::Channel, Request};

pub struct Hotspot {
    client: PocMobileClient<Channel>,
    api_token: String,
    keypair: Keypair,
    serial: String,
}

impl Hotspot {
    pub async fn new(api_token: String) -> Self {
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
            api_token: format!("Bearer {api_token}"),
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

        let request = self.set_metadata(speedtest_req.clone());
        tracing::debug!("submitting speedtest {:?}", speedtest_req);

        let res = self.client.submit_speedtest(request).await?;
        tracing::debug!("submitted speedtest {:?}", res);

        Ok(())
    }

    pub fn set_metadata<T>(&self, inner: T) -> Request<T> {
        let mut request = tonic::Request::new(inner);
        let api_token = self.api_token.clone();
        let metadata_value = MetadataValue::from_str(api_token.as_str()).unwrap();

        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        request
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
