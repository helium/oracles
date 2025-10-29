use crate::common::{make_keypair, spawn_gateway_service};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use h3o::{LatLng, Resolution};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{self as proto, GatewayClient, GatewayInfo},
    BlockchainRegionParamsV1, Region,
};
use hextree::Cell;
use iot_config::{gateway::db::Gateway, region_map};
use libflate::gzip::Encoder;
use prost::Message;
use sqlx::PgPool;
use std::io::Write;
use std::vec;
use tonic::Code;

const DEFAULT_REGION: Region = Region::Us915;

#[sqlx::test]
async fn gateway_info_v1_authorization_errors(pool: PgPool) -> anyhow::Result<()> {
    // NOTE: The information we're requesting does not exist in the DB for
    // this test. But we're only interested in Authization Errors.

    let admin_key = make_keypair(); // unlimited access
    let gw_key = make_keypair(); // access to self
    let unknown_key = make_keypair(); // no access

    // Start the gateway server
    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    // Connect with the assigned address
    let mut client = GatewayClient::connect(addr).await?;

    // Request gateway info as administrator
    let req = make_signed_info_request(gw_key.public_key(), &admin_key);
    let err = client.info(req).await.expect_err("testing expects error");
    assert_ne!(
        err.code(),
        Code::PermissionDenied,
        "admins have full access"
    );

    // Request gateway from unknown key
    let req = make_signed_info_request(gw_key.public_key(), &unknown_key);
    let err = client.info(req).await.expect_err("testing expects errors");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "unknown keys are denied"
    );

    // Request self with a different signer
    let mut req = make_signed_info_request(gw_key.public_key(), &gw_key);
    req.signature = vec![];
    req.signature = admin_key.sign(&req.encode_to_vec()).unwrap();
    let err = client.info(req).await.expect_err("testing expects errors");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "signature must match signer"
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_location_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let pub_key = make_keypair().public_key().clone();
    let now = Utc::now();

    let gateway = insert_gateway(&pool, now, pub_key.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    let res = req_gateway_location_v1(&mut client, &pub_key, &admin_key).await?;

    let cell = Cell::from_raw(gateway.location.unwrap() as u64)?;
    assert_eq!(res.location, cell.to_string());

    Ok(())
}

#[sqlx::test]
async fn gateway_region_params_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let keypair = make_keypair();
    let pub_key = keypair.public_key().clone();
    let now = Utc::now();

    let gateway = insert_gateway(&pool, now, pub_key.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    let res = req_gateway_region_params_v1(&mut client, &pub_key, &keypair).await?;

    assert_eq!(res.region, DEFAULT_REGION as i32);
    assert_eq!(
        res.params,
        Some(BlockchainRegionParamsV1 {
            region_params: vec![],
        })
    );
    assert_eq!(res.gain, gateway.gain.unwrap() as u64);

    Ok(())
}

#[sqlx::test]
async fn gateway_info_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let pub_key = make_keypair().public_key().clone();
    let now = Utc::now();

    let gateway = insert_gateway(&pool, now, pub_key.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    let res = req_gateway_info_v1(&mut client, &pub_key, &admin_key).await?;

    assert!(res.info.is_some());

    let info = res.info.unwrap();
    assert_eq!(info.address, pub_key.to_vec());
    assert_eq!(info.is_full_hotspot, gateway.is_full_hotspot.unwrap());
    assert!(info.metadata.is_some());

    let metadata = info.metadata.unwrap();
    let cell = Cell::from_raw(gateway.location.unwrap() as u64)?;
    assert_eq!(metadata.location, cell.to_string());
    assert_eq!(metadata.region, Region::Us915 as i32);
    assert_eq!(metadata.gain, gateway.gain.unwrap() as i32);
    assert_eq!(metadata.elevation, gateway.elevation.unwrap() as i32);

    Ok(())
}

#[sqlx::test]
async fn gateway_info_stream_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let pub_key = make_keypair().public_key().clone();
    let now = Utc::now();

    let gateway = insert_gateway(&pool, now, pub_key.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    let res = req_gateway_info_stream_v1(&mut client, &admin_key).await?;

    assert_eq!(res.gateways.len(), 1);

    let info: &GatewayInfo = res.gateways.first().unwrap();

    assert_eq!(info.address, pub_key.to_vec());
    assert_eq!(info.is_full_hotspot, gateway.is_full_hotspot.unwrap());
    assert!(info.metadata.is_some());

    let metadata = info.metadata.clone().unwrap();
    let cell = Cell::from_raw(gateway.location.unwrap() as u64)?;
    assert_eq!(metadata.location, cell.to_string());
    assert_eq!(metadata.region, Region::Us915 as i32);
    assert_eq!(metadata.gain, gateway.gain.unwrap() as i32);
    assert_eq!(metadata.elevation, gateway.elevation.unwrap() as i32);

    Ok(())
}

#[sqlx::test]
async fn gateway_info_stream_v2(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let pub_key1 = make_keypair().public_key().clone();
    let pub_key2 = make_keypair().public_key().clone();
    let now_min_15 = Utc::now() - chrono::Duration::minutes(15);
    let now_min_10 = Utc::now() - chrono::Duration::minutes(10);

    let gateway1 = insert_gateway(&pool, now_min_15, pub_key1.clone().into()).await?;
    let gateway2 = insert_gateway(&pool, now_min_10, pub_key2.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    // Get them ALL
    let res = req_gateway_info_stream_v2(&mut client, &admin_key, 0, 0).await?;

    assert_eq!(res.gateways.len(), 2);

    let info: &GatewayInfo = res.gateways.first().unwrap();

    assert_eq!(info.address, pub_key1.to_vec());
    assert_eq!(info.is_full_hotspot, gateway1.is_full_hotspot.unwrap());
    assert!(info.metadata.is_some());

    let metadata = info.metadata.clone().unwrap();
    let cell = Cell::from_raw(gateway1.location.unwrap() as u64)?;
    assert_eq!(metadata.location, cell.to_string());
    assert_eq!(metadata.region, Region::Us915 as i32);
    assert_eq!(metadata.gain, gateway1.gain.unwrap() as i32);
    assert_eq!(metadata.elevation, gateway1.elevation.unwrap() as i32);

    // Get min_updated_at = now_min_10
    let res = req_gateway_info_stream_v2(&mut client, &admin_key, now_min_10.timestamp() as u64, 0)
        .await?;

    assert_eq!(res.gateways.len(), 1);

    let info: &GatewayInfo = res.gateways.first().unwrap();

    assert_eq!(info.address, pub_key2.to_vec());
    assert_eq!(info.is_full_hotspot, gateway2.is_full_hotspot.unwrap());
    assert!(info.metadata.is_some());

    let metadata = info.metadata.clone().unwrap();
    let cell = Cell::from_raw(gateway2.location.unwrap() as u64)?;
    assert_eq!(metadata.location, cell.to_string());
    assert_eq!(metadata.region, Region::Us915 as i32);
    assert_eq!(metadata.gain, gateway2.gain.unwrap() as i32);
    assert_eq!(metadata.elevation, gateway2.elevation.unwrap() as i32);

    // Get min_location_changed_at = now_min_10
    let res = req_gateway_info_stream_v2(&mut client, &admin_key, 0, now_min_10.timestamp() as u64)
        .await?;

    assert_eq!(res.gateways.len(), 1);

    let info: &GatewayInfo = res.gateways.first().unwrap();

    assert_eq!(info.address, pub_key2.to_vec());
    assert_eq!(info.is_full_hotspot, gateway2.is_full_hotspot.unwrap());
    assert!(info.metadata.is_some());

    let metadata = info.metadata.clone().unwrap();
    let cell = Cell::from_raw(gateway2.location.unwrap() as u64)?;
    assert_eq!(metadata.location, cell.to_string());
    assert_eq!(metadata.region, Region::Us915 as i32);
    assert_eq!(metadata.gain, gateway2.gain.unwrap() as i32);
    assert_eq!(metadata.elevation, gateway2.elevation.unwrap() as i32);

    Ok(())
}

fn make_signed_info_request(address: &PublicKey, signer: &Keypair) -> proto::GatewayInfoReqV1 {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}

async fn req_gateway_location_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    address: &PublicKey,
    signer: &Keypair,
) -> anyhow::Result<proto::GatewayLocationResV1> {
    let mut req = proto::GatewayLocationReqV1 {
        gateway: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();

    let res = client.location(req).await?.into_inner();
    Ok(res)
}

async fn req_gateway_region_params_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    address: &PublicKey,
    signer: &Keypair,
) -> anyhow::Result<proto::GatewayRegionParamsResV1> {
    let mut req = proto::GatewayRegionParamsReqV1 {
        region: 0,
        address: address.to_vec(),
        signature: vec![],
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();

    let res = client.region_params(req).await?.into_inner();
    Ok(res)
}

async fn req_gateway_info_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    address: &PublicKey,
    signer: &Keypair,
) -> anyhow::Result<proto::GatewayInfoResV1> {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();

    let res = client.info(req).await?.into_inner();
    Ok(res)
}

async fn req_gateway_info_stream_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
) -> anyhow::Result<proto::GatewayInfoStreamResV1> {
    let mut req = proto::GatewayInfoStreamReqV1 {
        batch_size: 10_000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();

    let mut stream = client.info_stream(req).await?.into_inner();

    let first = stream
        .next()
        .await
        .transpose()? // map tonic Status into Err
        .ok_or_else(|| anyhow::Error::msg("no response"))?;

    Ok(first)
}

async fn req_gateway_info_stream_v2(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    min_updated_at: u64,
    min_location_changed_at: u64,
) -> anyhow::Result<proto::GatewayInfoStreamResV1> {
    let mut req = proto::GatewayInfoStreamReqV2 {
        batch_size: 10_000,
        min_updated_at,
        min_location_changed_at,
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();

    let mut stream = client.info_stream_v2(req).await?.into_inner();

    let first = stream
        .next()
        .await
        .transpose()? // map tonic Status into Err
        .ok_or_else(|| anyhow::Error::msg("no response"))?;

    Ok(first)
}

async fn insert_gateway(
    pool: &PgPool,
    now: DateTime<Utc>,
    pub_key: PublicKeyBinary,
) -> anyhow::Result<Gateway> {
    let sf = LatLng::new(37.7749, -122.4194)?; // San Francisco
    let cell = sf.to_cell(Resolution::Twelve); // resolution 12
    let h3_index: u64 = cell.into(); // u64

    let gateway = Gateway {
        address: pub_key.clone(),
        created_at: now,
        elevation: Some(1),
        gain: Some(2),
        hash: "hash1".to_string(),
        is_active: Some(true),
        is_full_hotspot: Some(true),
        last_changed_at: now,
        location: Some(h3_index),
        location_asserts: Some(1),
        location_changed_at: Some(now),
        refreshed_at: Some(now),
        updated_at: now,
    };

    Gateway::insert_bulk(pool, std::slice::from_ref(&gateway)).await?;

    let loc_bytes = h3_index.to_le_bytes();
    let mut encoder = Encoder::new(Vec::new())?;
    encoder.write_all(&loc_bytes)?;
    let compressed = encoder.finish().into_result()?;

    region_map::update_region(
        DEFAULT_REGION,
        &BlockchainRegionParamsV1 {
            region_params: vec![],
        },
        Some(&compressed),
        pool,
    )
    .await?;

    Ok(gateway)
}
