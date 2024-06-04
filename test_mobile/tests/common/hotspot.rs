use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use chrono::Utc;
use h3o::CellIndex;
use helium_crypto::{KeyTag, Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::services::{
    mobile_config::DeviceType,
    poc_mobile::{
        Client as PocMobileClient, CoverageObjectReqV1, RadioHexSignalLevel, SpeedtestReqV1,
        WifiHeartbeatReqV1,
    },
};
use prost::Message;
use rand::rngs::OsRng;
use sqlx::postgres::PgPoolOptions;
use std::str::FromStr;
use tonic::{metadata::MetadataValue, transport::Channel, Request};
use uuid::Uuid;

use crate::common::{hours_ago, now, TimestampToDateTime};

pub struct Hotspot {
    mobile_client: PocMobileClient<Channel>,
    api_token: String,
    keypair: Keypair,
    wallet: Keypair,
    serial: String,
    location: CellIndex,
}

#[test]
fn it_works() {
    let keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
    let x: Vec<u8> = keypair.public_key().into();
    println!("{:?}", keypair);
    println!("{:?}", x);
    println!("{:?}", keypair.public_key().to_vec());
    println!("{:?}", bs58::encode(x.clone()).into_string());
    println!(
        "{:?}",
        PublicKey::from_bytes(x.clone()).unwrap().to_string()
    );
    println!("{:?}", PublicKeyBinary::from(x).to_string());
}

impl Hotspot {
    pub async fn new(api_token: String, index: u64) -> Result<Self> {
        let endpoint = "http://127.0.0.1:9080";

        let client = (|| PocMobileClient::connect(endpoint))
            .retry(&ExponentialBuilder::default())
            .await
            .expect("client connect");

        let keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
        let wallet = Keypair::generate(KeyTag::default(), &mut OsRng);

        let b58 = keypair.public_key().to_string();

        tracing::info!("hotspot {b58} connected to ingester");

        let location = h3o::CellIndex::try_from(index).unwrap();

        let result = populate_mobile_metadata(&keypair, &wallet, location).await?;

        tracing::info!("{:?}", result);

        Ok(Self {
            mobile_client: client,
            api_token: format!("Bearer {api_token}"),
            keypair,
            wallet,
            serial: b58,
            location,
        })
    }

    pub async fn submit_speedtest(
        &mut self,
        upload_speed: u64,
        download_speed: u64,
        latency: u32,
    ) -> Result<()> {
        let timestamp = now();

        let mut speedtest_req = SpeedtestReqV1 {
            pub_key: self.keypair.public_key().to_vec(),
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

        let res = self.mobile_client.submit_speedtest(request).await?;
        tracing::debug!(
            "submitted speedtest @ {} = {:?}",
            timestamp.to_datetime(),
            res
        );

        Ok(())
    }

    pub async fn submit_coverage_object(&mut self, uuid: Uuid) -> Result<()> {
        let coverage_claim_time = now() - hours_ago(24);

        let mut coverage_object_req = CoverageObjectReqV1 {
            pub_key: self.keypair.public_key().to_vec(),
            uuid: uuid.as_bytes().to_vec(),
            coverage_claim_time,
            coverage: vec![RadioHexSignalLevel {
                location: self.location.to_string(),
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

        let res = self.mobile_client.submit_coverage_object(request).await?;
        tracing::debug!(
            "submitted coverage_object @ {} = {:?}",
            coverage_claim_time.to_datetime(),
            res
        );

        Ok(())
    }

    pub async fn submit_wifi_heartbeat(&mut self, when: u64, coverage_object: Uuid) -> Result<()> {
        let timestamp = now() - when;

        let center_loc = self
            .location
            .center_child(h3o::Resolution::Thirteen)
            .expect("center child");

        let lat_lon = h3o::LatLng::from(center_loc);

        let mut wifi_heartbeat_req = WifiHeartbeatReqV1 {
            pub_key: self.keypair.public_key().to_vec(),
            timestamp,
            // lat: 19.642310,
            // lon: -155.990626,
            lat: lat_lon.lat(),
            lon: lat_lon.lng(),
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

        let res = self.mobile_client.submit_wifi_heartbeat(request).await?;
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

impl Drop for Hotspot {
    fn drop(&mut self) {
        tracing::debug!("Hotspot dropped")
    }
}

async fn populate_mobile_metadata(
    keypair: &Keypair,
    wallet: &Keypair,
    location: CellIndex,
) -> Result<()> {
    let database_url = "postgres://postgres:postgres@localhost:5432/mobile_metadata";

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    let uuid = Uuid::new_v4();
    let h3_index: u64 = location.into();
    let device_type = DeviceType::WifiIndoor;
    let device_type_json = serde_json::to_value(&device_type)?;

    let wallet_b58 = wallet.public_key().to_string();

    sqlx::query(
        r#"
        INSERT INTO mobile_hotspot_infos (
            address, asset, bump_seed, 
            location, is_full_hotspot, num_location_asserts, 
            refreshed_at, created_at, is_active, 
            dc_onboarding_fee_paid, device_type
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(wallet_b58.as_str()) // address
    .bind(uuid.to_string()) // asset
    .bind(254) // bump_seed
    .bind(h3_index as i64) // location
    .bind(true) // is_full_hotspot
    .bind(1) // num_location_asserts
    .bind(Utc::now()) // refreshed_at
    .bind(Utc::now()) // created_at
    .bind(true) // is_active
    .bind(400000) // dc_onboarding_fee_paid
    .bind(device_type_json) // device_type
    .execute(&pool)
    .await?;

    // let pk_binary: PublicKeyBinary = keypair.public_key().to_owned().into();
    let pk_binary = PublicKeyBinary::from_str(keypair.public_key().to_string().as_str())?;
    let entity_key = bs58::decode(pk_binary.to_string()).into_vec()?;

    sqlx::query(
        r#"
        INSERT INTO key_to_assets (
            address, asset, bump_seed, 
            created_at, dao, entity_key, 
            refreshed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(wallet_b58.as_str()) // address
    .bind(uuid.to_string()) // asset
    .bind(254) // bump_seed
    .bind(Utc::now()) // created_at
    .bind("BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie") // dao
    .bind(entity_key) // entity_key
    .bind(Utc::now()) // refreshed_at
    .execute(&pool)
    .await?;

    Ok(())
}
