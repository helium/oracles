use crate::common::{gateway_db::TestGatewayBuilder, make_keypair, spawn_gateway_service};
use chrono::{Duration, Utc};
use futures::stream::StreamExt;
use helium_crypto::{Keypair, Sign};
use helium_proto::services::mobile_config::{
    self as proto, DeviceTypeV2, GatewayClient, GatewayInfoStreamReqV4, GatewayInfoV4,
};
use mobile_config::gateway::db::{Gateway, GatewayType};
use prost::Message;
use sqlx::PgPool;
use std::vec;

#[sqlx::test]
async fn gateway_stream_info_v4_basic(pool: PgPool) -> anyhow::Result<()> {
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
        .owner(Some("owner1".to_string()))
        .owner_changed_at(Some(now))
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
        .owner(Some("owner2".to_string()))
        .owner_changed_at(Some(now_plus_10))
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Select all devices
    let resp = gateway_info_stream_v4(&mut client, &admin_key, &[], 0, 0, 0).await?;
    assert_eq!(resp.gateways.len(), 2);

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v4_filters_null_owner(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let address2 = make_keypair().public_key().clone();

    let now = Utc::now();
    let now_plus_10 = now + Duration::seconds(10);

    // Gateway with owner set
    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now)
        .inserted_at(now)
        .refreshed_at(now)
        .last_changed_at(now_plus_10)
        .owner(Some("owner1".to_string()))
        .owner_changed_at(Some(now))
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    // Gateway without owner (NULL)
    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiOutdoor)
        .created_at(now)
        .inserted_at(now)
        .refreshed_at(now)
        .last_changed_at(now_plus_10)
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Only the gateway with owner should be returned
    let resp = gateway_info_stream_v4(&mut client, &admin_key, &[], 0, 0, 0).await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV4 = resp.gateways.first().unwrap();
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.owner, "owner1");

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v4_min_owner_changed_at(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let address2 = make_keypair().public_key().clone();

    let now = Utc::now();
    let now_minus_six = now - Duration::hours(6);
    let now_minus_four = now - Duration::hours(4);
    let now_minus_three = now - Duration::hours(3);
    let now_minus_five = now - Duration::hours(5);

    // Gateway1: owner changed at now - 6 hours (older)
    let gateway1: Gateway = TestGatewayBuilder::default()
        .address(address1.clone())
        .gateway_type(GatewayType::WifiIndoor)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now)
        .last_changed_at(now_minus_three)
        .owner(Some("owner1".to_string()))
        .owner_changed_at(Some(now_minus_six))
        .build()?
        .into();
    gateway1.insert(&pool).await?;

    // Gateway2: owner changed at now - 4 hours (newer)
    let gateway2: Gateway = TestGatewayBuilder::default()
        .address(address2.clone())
        .gateway_type(GatewayType::WifiOutdoor)
        .created_at(now_minus_six)
        .inserted_at(now_minus_six)
        .refreshed_at(now)
        .last_changed_at(now_minus_three)
        .owner(Some("owner2".to_string()))
        .owner_changed_at(Some(now_minus_four))
        .build()?
        .into();
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // min_owner_changed_at = now - 5 hours. Only gateway2 (changed at now - 4h) should pass
    let resp = gateway_info_stream_v4(
        &mut client,
        &admin_key,
        &[],
        0,
        0,
        now_minus_five.timestamp() as u64,
    )
    .await?;
    assert_eq!(resp.gateways.len(), 1);

    let gateway: &GatewayInfoV4 = resp.gateways.first().unwrap();
    assert_eq!(gateway.address, address2.to_vec());
    assert_eq!(gateway.owner, "owner2");

    // min_owner_changed_at = now - 6 hours both should pass
    let resp = gateway_info_stream_v4(
        &mut client,
        &admin_key,
        &[],
        0,
        0,
        now_minus_six.timestamp() as u64,
    )
    .await?;
    assert_eq!(resp.gateways.len(), 2);

    Ok(())
}

async fn gateway_info_stream_v4(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    device_types: &[DeviceTypeV2],
    min_updated_at: u64,
    min_location_changed_at: u64,
    min_owner_changed_at: u64,
) -> anyhow::Result<proto::GatewayInfoStreamResV4> {
    let mut req = GatewayInfoStreamReqV4 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types
            .iter()
            .map(|v| DeviceTypeV2::into(*v))
            .collect(),
        min_updated_at,
        min_location_changed_at,
        min_owner_changed_at,
    };
    req.signature = signer.sign(&req.encode_to_vec())?;
    let mut stream = client.info_stream_v4(req).await?.into_inner();

    let first = stream
        .next()
        .await
        .transpose()? // map tonic Status into Err
        .ok_or_else(|| anyhow::Error::msg("no response"))?;

    Ok(first)
}
