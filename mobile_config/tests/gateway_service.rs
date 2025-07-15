use chrono::{Duration, Utc};
use futures::stream::StreamExt;
use std::vec;

use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::services::mobile_config::{
    self as proto, gateway_metadata_v2::DeploymentInfo, DeviceType, DeviceTypeV2, GatewayClient,
    GatewayInfoStreamReqV1, GatewayInfoStreamReqV2, GatewayInfoStreamReqV3, GatewayInfoStreamResV2,
};
use mobile_config::{
    gateway_service::GatewayService,
    key_cache::{CacheKeys, KeyCache},
    KeyRole,
};
use prost::Message;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tonic::{transport, Code};

pub mod common;
use common::*;

#[sqlx::test]
async fn gateway_info_authorization_errors(pool: PgPool) -> anyhow::Result<()> {
    // NOTE(mj): The information we're requesting does not exist in the DB for
    // this test. But we're only interested in Authization Errors.

    let admin_key = make_keypair(); // unlimited access
    let gw_key = make_keypair(); // access to self
    let unknown_key = make_keypair(); // no access
    let server_key = make_keypair(); // signs responses

    // Let the OS assign a port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Start the gateway server
    let keys = CacheKeys::from_iter([(admin_key.public_key().to_owned(), KeyRole::Administrator)]);
    let (_key_cache_tx, key_cache) = KeyCache::new(keys);
    let gws = GatewayService::new(key_cache, pool.clone(), server_key, pool.clone());
    let _handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    // Connect with the assigned address
    let mut client = GatewayClient::connect(format!("http://{addr}")).await?;

    // Request information about ourselves
    let req = make_signed_info_request(gw_key.public_key(), &gw_key);
    let err = client.info(req).await.expect_err("testing expects error");
    assert_ne!(
        err.code(),
        Code::PermissionDenied,
        "gateway can request infomation about itself"
    );

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

async fn spawn_gateway_service(
    pool: PgPool,
    admin_pub_key: PublicKey,
) -> (
    String,
    tokio::task::JoinHandle<std::result::Result<(), helium_proto::services::Error>>,
) {
    let server_key = make_keypair();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start the gateway server
    let keys = CacheKeys::from_iter([(admin_pub_key.to_owned(), KeyRole::Administrator)]);
    let (_key_cache_tx, key_cache) = KeyCache::new(keys);
    let gws = GatewayService::new(key_cache, pool.clone(), server_key, pool.clone());
    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    (format!("http://{addr}"), handle)
}

#[sqlx::test]
async fn gateway_stream_info_v1(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        None,
    )
    .await;
    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now_plus_10,
        Some(now_plus_10),
        None,
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Select all devices
    let req = make_gateway_stream_signed_req_v1(&admin_key, &[]);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 2);

    // Filter by device type
    let req = make_gateway_stream_signed_req_v1(&admin_key, &[DeviceType::WifiIndoor]);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceType::WifiIndoor)
    );
}

#[sqlx::test]
async fn gateway_stream_info_v2(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        None,
    )
    .await;
    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now_plus_10,
        Some(now_plus_10),
        None,
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Select all devices
    let req = make_gateway_stream_signed_req_v2(&admin_key, &[], 0);
    let mut stream = client.info_stream_v2(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 2);

    // Filter by device type
    let req = make_gateway_stream_signed_req_v2(&admin_key, &[DeviceType::WifiIndoor], 0);
    let mut stream = client.info_stream_v2(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceType::WifiIndoor)
    );
}

#[sqlx::test]
async fn gateway_stream_info_v3(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset1_pubkey.clone().into(), now).await;

    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now_plus_10,
        Some(now_plus_10),
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset2_pubkey.clone().into(), now_plus_10).await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Select all devices
    let req = make_gateway_stream_signed_req_v3(&admin_key, &[], 0);
    let mut stream = client.info_stream_v3(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 2);

    // Filter by device type
    let req = make_gateway_stream_signed_req_v3(&admin_key, &[DeviceTypeV2::Indoor], 0);
    let mut stream = client.info_stream_v3(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceTypeV2::Indoor)
    );
}

#[sqlx::test]
async fn gateway_stream_info_v2_updated_at(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(updated_at),
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset1_pubkey.clone().into(), updated_at).await;

    // Shouldn't be returned
    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        created_at,
        None,
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset2_pubkey.clone().into(), created_at).await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_gateway_stream_signed_req_v2(&admin_key, &[], updated_at.timestamp() as u64);
    let mut stream = client.info_stream_v2(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);

    let gw_info = resp.gateways.first().unwrap();
    let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
    assert_eq!(pub_key, asset1_pubkey.clone());
    assert_eq!(
        DeviceType::try_from(gw_info.device_type).unwrap(),
        DeviceType::WifiIndoor
    );
    assert_eq!(
        i64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        asset1_hex_idx
    );
    assert!(stream.next().await.is_none());
}

#[sqlx::test]
async fn gateway_info_batch_v2(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(updated_at),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        created_at,
        None,
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset2_pubkey.clone().into(), created_at).await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_signed_info_batch_request(
        &vec![asset1_pubkey.clone(), asset2_pubkey.clone()],
        &admin_key,
    );
    let stream = client.info_batch_v2(req).await.unwrap().into_inner();
    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;

    let gateways = resp.first().unwrap().gateways.clone();
    let gw1 = gateways
        .iter()
        .find(|v| v.address == asset1_pubkey.to_vec())
        .unwrap();

    let deployment_info = gw1.metadata.clone().unwrap().deployment_info.unwrap();

    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
            assert_eq!(v.electrical_down_tilt, 3);
            assert_eq!(v.mechanical_down_tilt, 4);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    let gw2 = gateways
        .iter()
        .find(|v| v.address == asset2_pubkey.to_vec())
        .unwrap();
    assert!(gw2.metadata.clone().unwrap().deployment_info.is_none());
}

#[sqlx::test]
async fn gateway_info_batch_v2_updated_at_check(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let asset2_hex_idx = 631711286145955327_i64;
    let asset3_hex_idx = 631711286145006591_i64;
    let asset3_pubkey = make_keypair().public_key().clone();
    let asset4_pubkey = make_keypair().public_key().clone();
    let asset4_hex_idx = 0x8c44a82aed527ff_i64;

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);
    let updated_at = Utc::now() - Duration::hours(4);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiIndoor\"",
        asset2_pubkey.clone().into(),
        created_at,
        None,
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset3",
        Some(asset3_hex_idx),
        "\"wifiDataOnly\"",
        asset3_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset3_pubkey.clone().into(), updated_at).await;

    // Must be ignored since not included in req
    add_db_record(
        &pool,
        "asset4",
        Some(asset4_hex_idx),
        "\"wifiIndoor\"",
        asset4_pubkey.clone().into(),
        created_at,
        None,
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_signed_info_batch_request(
        &vec![
            asset1_pubkey.clone(),
            asset2_pubkey.clone(),
            asset3_pubkey.clone(),
            make_keypair().public_key().clone(), // it doesn't exist
        ],
        &admin_key,
    );
    let stream = client.info_batch_v2(req).await.unwrap().into_inner();
    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 3);
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset1_pubkey.to_vec())
            .unwrap()
            .updated_at,
        refreshed_at.timestamp() as u64
    );
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset2_pubkey.to_vec())
            .unwrap()
            .updated_at,
        created_at.timestamp() as u64
    );

    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset3_pubkey.to_vec())
            .unwrap()
            .updated_at,
        updated_at.timestamp() as u64
    );
}

#[sqlx::test]
async fn gateway_info_v2_no_mobile_tracker_record(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let asset2_hex_idx = 631711286145955327_i64;

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiIndoor\"",
        asset2_pubkey.clone().into(),
        created_at,
        None,
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    // asset 1
    let req = make_signed_info_request(&asset1_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();
    let gw_info = resp.info.unwrap();
    let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
    assert_eq!(pub_key, asset1_pubkey.clone());
    assert_eq!(gw_info.updated_at, refreshed_at.timestamp() as u64);

    // asset 2
    let req = make_signed_info_request(&asset2_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();
    let gw_info = resp.info.unwrap();
    let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
    assert_eq!(pub_key, asset2_pubkey.clone());
    assert_eq!(gw_info.updated_at, created_at.timestamp() as u64);
}

#[sqlx::test]
async fn gateway_info_v2(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(updated_at),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;
    add_mobile_tracker_record(&pool, asset1_pubkey.clone().into(), updated_at).await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_signed_info_request(&asset1_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();

    let gw_info = resp.info.unwrap();
    let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
    assert_eq!(pub_key, asset1_pubkey.clone());
    assert_eq!(
        DeviceType::try_from(gw_info.device_type).unwrap(),
        DeviceType::WifiIndoor
    );
    assert_eq!(gw_info.updated_at, updated_at.timestamp() as u64);
    assert_eq!(
        i64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        asset1_hex_idx
    );

    let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();

    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
            assert_eq!(v.electrical_down_tilt, 3);
            assert_eq!(v.mechanical_down_tilt, 4);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    // Non-existent
    let req = make_signed_info_request(&asset2_pubkey, &admin_key);
    let resp_err = client
        .info_v2(req)
        .await
        .expect_err("testing expects error");

    assert_eq!(resp_err.code(), Code::NotFound);
}

#[sqlx::test]
async fn gateway_info_stream_v2_updated_at_check(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let asset2_hex_idx = 631711286145955327_i64;
    let asset3_hex_idx = 631711286145006591_i64;
    let asset3_pubkey = make_keypair().public_key().clone();

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);
    let updated_at = Utc::now() - Duration::hours(4);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiIndoor\"",
        asset2_pubkey.clone().into(),
        created_at,
        None,
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset3",
        Some(asset3_hex_idx),
        "\"wifiDataOnly\"",
        asset3_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        None,
    )
    .await;
    add_mobile_tracker_record(&pool, asset3_pubkey.clone().into(), updated_at).await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_gateway_stream_signed_req_v2(&admin_key, &[], 0);
    let stream = client.info_stream_v2(req).await.unwrap().into_inner();

    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 3);
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset1_pubkey.to_vec())
            .unwrap()
            .updated_at,
        refreshed_at.timestamp() as u64
    );
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset2_pubkey.to_vec())
            .unwrap()
            .updated_at,
        created_at.timestamp() as u64
    );

    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == asset3_pubkey.to_vec())
            .unwrap()
            .updated_at,
        updated_at.timestamp() as u64
    );
}

#[sqlx::test]
async fn gateway_stream_info_v2_deployment_info(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let asset2_hex_idx = 631711286145955327_i64;
    let asset3_hex_idx = 631711286145006591_i64;
    let asset2_pubkey = make_keypair().public_key().clone();
    let asset3_pubkey = make_keypair().public_key().clone();
    let now = Utc::now();

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;
    add_db_record(
        &pool,
        "asset2",
        Some(asset2_hex_idx),
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now,
        Some(now),
        // Should be returned None in deployment info
        Some(r#"{"wifiInfoV0Invalid": {"antenna": 18}}"#),
    )
    .await;
    add_db_record(
        &pool,
        "asset3",
        Some(asset3_hex_idx),
        "\"wifiDataOnly\"",
        asset3_pubkey.clone().into(),
        now,
        Some(now),
        None,
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;

    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Check wifi indoor
    let req = make_gateway_stream_signed_req_v2(&admin_key, &[DeviceType::WifiIndoor], 0);
    let mut stream = client.info_stream_v2(req).await.unwrap().into_inner();
    let res = stream.next().await.unwrap().unwrap();
    let gw_info = res.gateways.first().unwrap();
    let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
    assert_eq!(pub_key, asset1_pubkey.clone());
    assert_eq!(
        DeviceType::try_from(gw_info.device_type).unwrap(),
        DeviceType::WifiIndoor
    );
    assert_eq!(
        i64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        asset1_hex_idx
    );
    assert!(stream.next().await.is_none());

    // Check wifi data only
    let req = make_gateway_stream_signed_req_v2(&admin_key, &[DeviceType::WifiDataOnly], 0);
    let stream = client.info_stream_v2(req).await.unwrap().into_inner();

    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 2);
    let device_type = gateways.first().unwrap().device_type;
    assert_eq!(
        DeviceType::try_from(device_type).unwrap(),
        DeviceType::WifiDataOnly
    );
    let device_type = gateways.get(1).unwrap().device_type;
    assert_eq!(
        DeviceType::try_from(device_type).unwrap(),
        DeviceType::WifiDataOnly
    );

    // Check all
    let req = make_gateway_stream_signed_req_v2(&admin_key, &[], 0);
    let stream = client.info_stream_v2(req).await.unwrap().into_inner();

    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();

    // Check deployment info
    assert_eq!(gateways.len(), 3);
    for gw in gateways {
        if let Some(metadata) = &gw.metadata {
            if DeviceType::try_from(gw.device_type).unwrap() != DeviceType::WifiIndoor {
                assert!(metadata.deployment_info.is_none());
            } else {
                let deployment_info = metadata.deployment_info.as_ref().unwrap();
                match deployment_info {
                    DeploymentInfo::WifiDeploymentInfo(v) => {
                        assert_eq!(v.antenna, 18);
                        assert_eq!(v.azimuth, 160);
                        assert_eq!(v.elevation, 5);
                        assert_eq!(v.electrical_down_tilt, 1);
                        assert_eq!(v.mechanical_down_tilt, 2);
                    }
                    DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
                };
            }
        }
    }
}

fn make_gateway_stream_signed_req_v3(
    signer: &Keypair,
    device_types: &[DeviceTypeV2],
    min_updated_at: u64,
) -> proto::GatewayInfoStreamReqV3 {
    let mut req = GatewayInfoStreamReqV3 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types
            .iter()
            .map(|v| DeviceTypeV2::into(*v))
            .collect(),
        min_updated_at,
        min_location_changed_at: 0, // TODO testme
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}

fn make_gateway_stream_signed_req_v2(
    signer: &Keypair,
    device_types: &[DeviceType],
    min_updated_at: u64,
) -> proto::GatewayInfoStreamReqV2 {
    let mut req = GatewayInfoStreamReqV2 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
        min_updated_at,
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}

fn make_gateway_stream_signed_req_v1(
    signer: &Keypair,
    device_types: &[DeviceType],
) -> proto::GatewayInfoStreamReqV1 {
    let mut req = GatewayInfoStreamReqV1 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
    };

    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
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

fn make_signed_info_batch_request(
    addresses: &[PublicKey],
    signer: &Keypair,
) -> proto::GatewayInfoBatchReqV1 {
    let mut req = proto::GatewayInfoBatchReqV1 {
        addresses: addresses.iter().map(|v| v.to_vec()).collect(),
        batch_size: 42,
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}
