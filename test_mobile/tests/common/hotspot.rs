use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use helium_crypto::{KeyTag, Keypair, Sign};
use helium_proto::services::poc_mobile::{
    Client as PocMobileClient, CoverageObjectReqV1, RadioHexSignalLevel, SpeedtestReqV1,
    WifiHeartbeatReqV1,
};
use prost::Message;
use rand::rngs::OsRng;
use std::str::FromStr;
use test_mobile::cli::assignment::CENTER_CELL;
use tonic::{metadata::MetadataValue, transport::Channel, Request};
use uuid::Uuid;

use crate::common::{hours_ago, keypair_to_bs58, now, TimestampToDateTime};

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
        let timestamp = now();

        let mut speedtest_req = SpeedtestReqV1 {
            pub_key: self.keypair.public_key().into(),
            serial: self.serial.clone(),
            timestamp,
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
        tracing::debug!(
            "submitting speedtest @ {} = {:?}",
            timestamp.to_datetime(),
            speedtest_req
        );

        let res = self.client.submit_speedtest(request).await?;
        tracing::debug!(
            "submitted speedtest @ {} = {:?}",
            timestamp.to_datetime(),
            res
        );

        Ok(())
    }

    pub async fn submit_coverage_object(&mut self, uuid: Uuid) -> Result<()> {
        let location = h3o::CellIndex::try_from(CENTER_CELL).unwrap();

        let coverage_claim_time = now() - hours_ago(24);

        let mut coverage_object_req = CoverageObjectReqV1 {
            pub_key: self.keypair.public_key().into(),
            uuid: uuid.as_bytes().to_vec(),
            coverage_claim_time,
            coverage: vec![RadioHexSignalLevel {
                location: location.to_string(),
                signal_level: 3,
                signal_power: 1000,
            }],
            indoor: false,
            trust_score: 1,
            signature: vec![],
            key_type: None,
        };

        coverage_object_req.signature = self
            .keypair
            .sign(&coverage_object_req.encode_to_vec())
            .expect("sign");

        let request = self.set_metadata(coverage_object_req.clone());
        tracing::debug!(
            "submitting coverage_object @ {} = {:?}",
            coverage_claim_time.to_datetime(),
            coverage_object_req
        );

        let res = self.client.submit_coverage_object(request).await?;
        tracing::debug!(
            "submitted coverage_object @ {} = {:?}",
            coverage_claim_time.to_datetime(),
            res
        );

        Ok(())
    }

    pub async fn submit_wifi_heartbeat(&mut self, when: u64, coverage_object: Uuid) -> Result<()> {
        let timestamp = now() - when;

        let mut wifi_heartbeat_req = WifiHeartbeatReqV1 {
            pub_key: self.keypair.public_key().into(),
            timestamp,
            lat: 19.642310,
            lon: -155.990626,
            location_validation_timestamp: timestamp,
            operation_mode: true,
            coverage_object: coverage_object.as_bytes().to_vec(),
            signature: vec![],
        };

        wifi_heartbeat_req.signature = self
            .keypair
            .sign(&wifi_heartbeat_req.encode_to_vec())
            .expect("sign");

        let request = self.set_metadata(wifi_heartbeat_req.clone());
        tracing::debug!(
            "submitting wifi_heartbeat @ {} = {:?}",
            timestamp.to_datetime(),
            wifi_heartbeat_req
        );

        let res = self.client.submit_wifi_heartbeat(request).await?;
        tracing::debug!(
            "submitted wifi_heartbeat @ {} = {:?}",
            timestamp.to_datetime(),
            res
        );

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
