use crate::common::{make_keypair, spawn_gateway_service};
use chrono::{DateTime, Duration, Utc};
use derive_builder::Builder;
use futures::stream::StreamExt;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::services::mobile_config::{
    self as proto, DeploymentInfo, DeviceTypeV2, GatewayClient, GatewayInfoStreamReqV3,
    GatewayInfoV3, LocationInfo,
};
use mobile_config::gateway::db::{Gateway, GatewayType};
use prost::Message;
use sqlx::PgPool;
use std::vec;

#[derive(Builder)]
#[builder(setter(into))]
struct TestGateway {
    address: PublicKey,
    gateway_type: GatewayType,
    #[builder(default)]
    created_at: DateTime<Utc>,
    #[builder(default)]
    inserted_at: DateTime<Utc>,
    #[builder(default)]
    refreshed_at: DateTime<Utc>,
    #[builder(default)]
    last_changed_at: DateTime<Utc>,
    #[builder(default)]
    hash: String,
    #[builder(default = "Some(18)")]
    antenna: Option<u32>,
    #[builder(default)]
    elevation: Option<u32>,
    #[builder(default)]
    azimuth: Option<u32>,
    #[builder(default)]
    location: Option<u64>,
    #[builder(default)]
    location_changed_at: Option<DateTime<Utc>>,
    #[builder(default)]
    location_asserts: Option<u32>,
    #[builder(default)]
    owner: Option<String>,
    #[builder(default)]
    hash_v2: Option<String>,
}

impl From<TestGateway> for Gateway {
    fn from(tg: TestGateway) -> Self {
        Gateway {
            address: tg.address.into(),
            gateway_type: tg.gateway_type,
            created_at: tg.created_at,
            inserted_at: tg.inserted_at,
            refreshed_at: tg.refreshed_at,
            last_changed_at: tg.last_changed_at,
            hash: tg.hash,
            antenna: tg.antenna,
            elevation: tg.elevation,
            azimuth: tg.azimuth,
            location: tg.location,
            location_changed_at: tg.location_changed_at,
            location_asserts: tg.location_asserts,
            owner: tg.owner,
            hash_v2: tg.hash_v2,
        }
    }
}

#[sqlx::test]
async fn gateway_stream_info_v3_basic(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let now = Utc::now();
    let now_plus_5 = now + Duration::seconds(5);
    let now_plus_10 = now + Duration::seconds(10);

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now)
        .inserted_at(now)
        .refreshed_at(now)
        .last_changed_at(now_plus_10)
        .elevation(2)
        .azimuth(161)
        .location(loc1)
        .location_changed_at(now_plus_5)
        .location_asserts(1)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiOutdoor)
        .created_at(now_plus_10)
        .inserted_at(now_plus_10)
        .refreshed_at(now_plus_10)
        .last_changed_at(now_plus_10)
        .location(loc2)
        .location_changed_at(now_plus_10)
        .location_asserts(1)
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Select all devices
    let resp = gateway_info_stream_v3(&mut client, &admin_key, &[], 0, 0).await?;
    assert_eq!(resp.gateways.len(), 2);

    // Filter by device type
    let resp =
        gateway_info_stream_v3(&mut client, &admin_key, &[DeviceTypeV2::Indoor], 0, 0).await?;
    assert_eq!(resp.gateways.len(), 1);
    let gateway: &GatewayInfoV3 = resp.gateways.first().unwrap();
    assert_eq!(gateway.device_type(), DeviceTypeV2::Indoor);
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.created_at, now.timestamp() as u64);
    assert_eq!(gateway.updated_at, now_plus_10.timestamp() as u64);
    assert_eq!(gateway.num_location_asserts, 1); // Has location, so should be 1
    assert_eq!(
        gateway.metadata.clone().unwrap().location_info.unwrap(),
        LocationInfo {
            location: format!("{:x}", loc1),
            location_changed_at: now_plus_5.timestamp() as u64
        }
    );
    assert_eq!(
        gateway.metadata.clone().unwrap().deployment_info.unwrap(),
        DeploymentInfo {
            antenna: 18,
            elevation: 2,
            azimuth: 161,
        }
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v3_no_metadata(pool: PgPool) -> anyhow::Result<()> {
    // There is location info but no deployment info
    let admin_key = make_keypair();
    let address1 = make_keypair().public_key().clone();
    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now)
        .inserted_at(now)
        .refreshed_at(now)
        .last_changed_at(now_plus_10)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let resp = gateway_info_stream_v3(&mut client, &admin_key, &[], 0, 0).await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV3 = resp.gateways.first().unwrap();
    assert_eq!(gateway.device_type(), DeviceTypeV2::Indoor);
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.created_at, now.timestamp() as u64);
    assert_eq!(gateway.updated_at, now_plus_10.timestamp() as u64);
    assert_eq!(gateway.num_location_asserts, 0); // No location, so should be 0
    assert!(gateway.metadata.is_none());

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v3_no_deployment_info(pool: PgPool) -> anyhow::Result<()> {
    // There is location info but no deployment info
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let now = Utc::now();
    let now_plus_5 = now + chrono::Duration::seconds(5);
    let now_plus_10 = now + chrono::Duration::seconds(10);

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now)
        .inserted_at(now)
        .refreshed_at(now)
        .last_changed_at(now_plus_10)
        .antenna(None)
        .location(loc1)
        .location_changed_at(now_plus_5)
        .location_asserts(1)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let resp = gateway_info_stream_v3(&mut client, &admin_key, &[], 0, 0).await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV3 = resp.gateways.first().unwrap();
    assert_eq!(gateway.device_type(), DeviceTypeV2::Indoor);
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.created_at, now.timestamp() as u64);
    assert_eq!(gateway.updated_at, now_plus_10.timestamp() as u64);
    assert_eq!(gateway.num_location_asserts, 1); // Has location, so should be 1
    assert_eq!(
        gateway.metadata.clone().unwrap().location_info.unwrap(),
        LocationInfo {
            location: format!("{:x}", loc1),
            location_changed_at: now_plus_5.timestamp() as u64
        }
    );
    println!(
        "gateway metadata: {:?}",
        gateway.metadata.clone().unwrap().deployment_info
    );
    assert!(gateway.metadata.clone().unwrap().deployment_info.is_none());

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v3_updated_at(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let inserted_at = Utc::now() - Duration::hours(3);

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(created_at)
        .inserted_at(inserted_at)
        .refreshed_at(inserted_at)
        .last_changed_at(inserted_at)
        .elevation(2)
        .azimuth(161)
        .location(loc1)
        .location_changed_at(inserted_at)
        .location_asserts(1)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiDataOnly)
        .created_at(created_at)
        .inserted_at(created_at)
        .refreshed_at(created_at)
        .last_changed_at(created_at)
        .location(loc2)
        .location_changed_at(created_at)
        .location_asserts(1)
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let resp = gateway_info_stream_v3(
        &mut client,
        &admin_key,
        &[],
        inserted_at.timestamp() as u64,
        0,
    )
    .await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV3 = resp.gateways.first().unwrap();
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.num_location_asserts, 1); // Has location, so should be 1
    assert_eq!(gateway.device_type(), DeviceTypeV2::Indoor);
    assert_eq!(
        u64::from_str_radix(
            &gateway
                .metadata
                .clone()
                .unwrap()
                .location_info
                .unwrap()
                .location,
            16
        )
        .unwrap(),
        loc1
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v3_min_location_changed_at_zero(pool: PgPool) -> anyhow::Result<()> {
    // address1 has no location
    // address2 has location
    // Make sure if min_location_changed_at == 0 then returned both radios
    // if min_location_changed_at >= 1 then radios with null location filtered out

    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let now = Utc::now();
    let now_minus_six = now - Duration::hours(6);
    let now_minus_four = now - Duration::hours(4);
    let now_minus_three = now - Duration::hours(3);

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now_minus_six)
        .last_changed_at(now_minus_three)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiDataOnly)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now_minus_six)
        .last_changed_at(now_minus_three)
        .location(loc2)
        .location_changed_at(now_minus_four)
        .location_asserts(1)
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let resp = gateway_info_stream_v3(&mut client, &admin_key, &[], 0, 0).await?;
    assert_eq!(resp.gateways.len(), 2);

    // min_location_changed_at = 1
    let resp = gateway_info_stream_v3(&mut client, &admin_key, &[], 0, 1).await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway = resp.gateways.first().unwrap();
    assert_eq!(gateway.address, address2.to_vec());

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v3_location_changed_at(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let now = Utc::now();
    let now_minus_six = now - Duration::hours(6);
    let now_minus_three = now - Duration::hours(3);
    let now_minus_four = now - Duration::hours(4);
    let now_minus_five = now - Duration::hours(5);

    // Scenario:
    // asset_1 location changed at now - 6 hours
    // asset_2 location changed at now - 4 hours
    // request min_location_changed_at location changed at now - 5 hour

    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now)
        .last_changed_at(now_minus_three)
        .elevation(2u32)
        .azimuth(161u32)
        .location(loc1)
        .location_changed_at(now_minus_six)
        .location_asserts(1)
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiDataOnly)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now)
        .last_changed_at(now_minus_three)
        .location(loc2)
        .location_changed_at(now_minus_four)
        .location_asserts(1)
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let resp = gateway_info_stream_v3(
        &mut client,
        &admin_key,
        &[],
        0,
        now_minus_five.timestamp() as u64,
    )
    .await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV3 = resp.gateways.first().unwrap();
    assert_eq!(gateway.address, address2.to_vec());
    assert_eq!(gateway.device_type(), DeviceTypeV2::DataOnly);
    assert_eq!(
        u64::from_str_radix(
            &gateway
                .metadata
                .clone()
                .unwrap()
                .location_info
                .unwrap()
                .location,
            16
        )
        .unwrap(),
        loc2
    );

    // Change min_location_changed_at parameter, now two radios should be returned
    let resp = gateway_info_stream_v3(
        &mut client,
        &admin_key,
        &[],
        0,
        now_minus_six.timestamp() as u64,
    )
    .await?;
    assert_eq!(resp.gateways.len(), 2);

    Ok(())
}

async fn gateway_info_stream_v3(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    device_types: &[DeviceTypeV2],
    min_inserted_at: u64,
    min_location_changed_at: u64,
) -> anyhow::Result<proto::GatewayInfoStreamResV3> {
    let mut req = GatewayInfoStreamReqV3 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types
            .iter()
            .map(|v| DeviceTypeV2::into(*v))
            .collect(),
        min_updated_at: min_inserted_at,
        min_location_changed_at,
    };
    req.signature = signer.sign(&req.encode_to_vec())?;
    let mut stream = client.info_stream_v3(req).await?.into_inner();

    let first = stream
        .next()
        .await
        .transpose()? // map tonic Status into Err
        .ok_or_else(|| anyhow::Error::msg("no response"))?;

    Ok(first)
}
