use chrono::{DateTime, Utc};
use futures::stream::StreamExt;

use helium_crypto::{KeyTag, Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::services::mobile_config::{
    self as proto, DeviceType, GatewayClient, GatewayInfoStreamReqV1, GatewayInfoStreamResV1,
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
    let gws = GatewayService::new(key_cache, pool.clone(), server_key);
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
    let gws = GatewayService::new(key_cache, pool, server_key);
    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    (format!("http://{addr}"), handle)
}

#[sqlx::test]
async fn gateway_stream_info_refreshed_at(pool: PgPool) {
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
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
    )
    .await;
    add_db_record(
        &pool,
        "asset2",
        asset2_hex_idx,
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now_plus_10,
        Some(now_plus_10),
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Regression test
    let req = make_gateway_stream_signed_req(&admin_key, &[], 0);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 2);

    // No device types but filter by refreshed_at
    let req = make_gateway_stream_signed_req(&admin_key, &[], now_plus_10.timestamp() as u64);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceType::WifiDataOnly)
    );

    // No refreshed_at but filter by device_type
    let req = make_gateway_stream_signed_req(&admin_key, &[DeviceType::WifiIndoor], 0);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceType::WifiIndoor)
    );

    // Filter by device_type and refreshed_at
    let req = make_gateway_stream_signed_req(
        &admin_key,
        &[DeviceType::WifiIndoor],
        now.timestamp() as u64,
    );
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);
    assert_eq!(
        resp.gateways.first().unwrap().device_type,
        Into::<i32>::into(DeviceType::WifiIndoor)
    );
}

#[sqlx::test]
async fn gateway_stream_info_refreshed_at_is_null(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let now = Utc::now();

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        None,
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();

    let req = make_gateway_stream_signed_req(&admin_key, &[], now.timestamp() as u64);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();

    // Make sure the gateway was returned
    let resp = stream.next().await.unwrap().unwrap();
    assert_eq!(resp.gateways.len(), 1);

    let req = make_gateway_stream_signed_req(&admin_key, &[], (now.timestamp() + 1) as u64);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
    // Response is empty
    assert!(stream.next().await.is_none());
}

// TODO two device_types test

#[sqlx::test]
async fn gateway_stream_info_data_types(pool: PgPool) {
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
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
    )
    .await;
    add_db_record(
        &pool,
        "asset2",
        asset2_hex_idx,
        "\"wifiDataOnly\"",
        asset2_pubkey.clone().into(),
        now,
        Some(now),
    )
    .await;
    add_db_record(
        &pool,
        "asset3",
        asset3_hex_idx,
        "\"wifiDataOnly\"",
        asset3_pubkey.clone().into(),
        now,
        Some(now),
    )
    .await;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;

    let mut client = GatewayClient::connect(addr).await.unwrap();

    // Check wifi indoor
    let req = make_gateway_stream_signed_req(&admin_key, &[DeviceType::WifiIndoor], 0);
    let mut stream = client.info_stream(req).await.unwrap().into_inner();
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
    let req = make_gateway_stream_signed_req(&admin_key, &[DeviceType::WifiDataOnly], 0);
    let stream = client.info_stream(req).await.unwrap().into_inner();

    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV1>>()
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
    let req = make_gateway_stream_signed_req(&admin_key, &[], 0);
    let stream = client.info_stream(req).await.unwrap().into_inner();

    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV1>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 3);
}

async fn add_db_record(
    pool: &PgPool,
    asset: &str,
    location: i64,
    device_type: &str,
    key: PublicKeyBinary,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
) {
    add_mobile_hotspot_infos(pool, asset, location, device_type, created_at, refreshed_at).await;
    add_asset_key(pool, asset, key).await;
}

async fn add_mobile_hotspot_infos(
    pool: &PgPool,
    asset: &str,
    location: i64,
    device_type: &str,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
) {
    sqlx::query(
        r#"
            INSERT INTO
"mobile_hotspot_infos" ("asset", "location", "device_type", "created_at", "refreshed_at")
            VALUES
($1, $2, $3::jsonb, $4, $5);
    "#,
    )
    .bind(asset)
    .bind(location)
    .bind(device_type)
    .bind(created_at)
    .bind(refreshed_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn add_asset_key(pool: &PgPool, asset: &str, key: PublicKeyBinary) {
    let b58 = bs58::decode(key.to_string()).into_vec().unwrap();
    sqlx::query(
        r#"
    INSERT INTO
    "key_to_assets" ("asset", "entity_key")
    VALUES ($1, $2);
    "#,
    )
    .bind(asset)
    .bind(b58)
    .execute(pool)
    .await
    .unwrap();
}

async fn create_db_tables(pool: &PgPool) {
    sqlx::query(
        r#"
        CREATE TABLE mobile_hotspot_infos (
        asset character varying(255) NULL,
        location numeric NULL,
        device_type jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        refreshed_at timestamptz
    );"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE key_to_assets (
            asset character varying(255) NULL,
            entity_key bytea NULL
        );"#,
    )
    .execute(pool)
    .await
    .unwrap();
}
fn make_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut rand::rngs::OsRng)
}

fn make_gateway_stream_signed_req(
    signer: &Keypair,
    device_types: &[DeviceType],
    min_refreshed_at: u64,
) -> proto::GatewayInfoStreamReqV1 {
    let mut req = GatewayInfoStreamReqV1 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
        min_refreshed_at,
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
