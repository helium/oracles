pub mod common;

use crate::common::make_keypair;
use chrono::{Duration, Utc};
use futures::stream::StreamExt;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::services::mobile_config::{
    self as proto, gateway_metadata_v2::DeploymentInfo, DeviceType, GatewayClient,
<<<<<<< HEAD
    GatewayInfoStreamReqV2, GatewayInfoStreamResV2,
=======
    GatewayInfoStreamReqV1, GatewayInfoStreamReqV2, GatewayInfoStreamResV1, GatewayInfoStreamResV2,
>>>>>>> a4695ad1 (More tests fix)
};
use mobile_config::{
    gateway::{
        db::{Gateway, GatewayType},
        service::GatewayService,
    },
    key_cache::{CacheKeys, KeyCache},
    KeyRole,
};
use prost::Message;
use sqlx::PgPool;
use std::{sync::Arc, vec};
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
    let gws = GatewayService::new(key_cache, pool.clone(), Arc::new(server_key), pool.clone());
    let _handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    // Connect with the assigned address
    let mut client = GatewayClient::connect(format!("http://{addr}")).await?;

    // Request information about ourselves
    let req = make_signed_info_request(gw_key.public_key(), &gw_key);
    let err = client
        .info_v2(req)
        .await
        .expect_err("testing expects error");
    assert_ne!(
        err.code(),
        Code::PermissionDenied,
        "gateway can request information about itself"
    );

    // Request gateway info as administrator
    let req = make_signed_info_request(gw_key.public_key(), &admin_key);
    let err = client
        .info_v2(req)
        .await
        .expect_err("testing expects error");
    assert_ne!(
        err.code(),
        Code::PermissionDenied,
        "admins have full access"
    );

    // Request gateway from unknown key
    let req = make_signed_info_request(gw_key.public_key(), &unknown_key);
    let err = client
        .info_v2(req)
        .await
        .expect_err("testing expects errors");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "unknown keys are denied"
    );

    // Request self with a different signer
    let mut req = make_signed_info_request(gw_key.public_key(), &gw_key);
    req.signature = vec![];
    req.signature = admin_key.sign(&req.encode_to_vec()).unwrap();
    let err = client
        .info_v2(req)
        .await
        .expect_err("testing expects errors");
    assert_eq!(
        err.code(),
        Code::PermissionDenied,
        "signature must match signer"
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: now,
        updated_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(now),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: now_plus_10,
        updated_at: now_plus_10,
        refreshed_at: now_plus_10,
        last_changed_at: now_plus_10,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(now_plus_10),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Select all devices
    let res = gateway_info_stream_v1(&mut client, &admin_key, &[]).await?;
    assert_eq!(res.gateways.len(), 2);

    // Filter by device type
    let res = gateway_info_stream_v1(&mut client, &admin_key, &[DeviceType::WifiIndoor]).await?;
    assert_eq!(res.gateways.len(), 1);
    assert_eq!(
        res.gateways.first().unwrap().device_type(),
        DeviceType::WifiIndoor
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v2(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let now = Utc::now();
    let now_plus_10 = now + chrono::Duration::seconds(10);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: now,
        updated_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(now),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: now_plus_10,
        updated_at: now_plus_10,
        refreshed_at: now_plus_10,
        last_changed_at: now_plus_10,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(now_plus_10),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Select all devices
    let res = gateway_info_stream_v2(&mut client, &admin_key, &[], 0).await?;
    assert_eq!(res.gateways.len(), 2);

    // Filter by device type
    let res = gateway_info_stream_v2(&mut client, &admin_key, &[DeviceType::WifiIndoor], 0).await?;
    assert_eq!(res.gateways.len(), 1);
    assert_eq!(
        res.gateways.first().unwrap().device_type(),
        DeviceType::WifiIndoor
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_stream_info_v2_updated_at(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: updated_at,
        refreshed_at: updated_at,
        last_changed_at: updated_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(updated_at),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: created_at,
        updated_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let res =
        gateway_info_stream_v2(&mut client, &admin_key, &[], updated_at.timestamp() as u64).await?;
    assert_eq!(res.gateways.len(), 1);

    let gateway = res.gateways.first().unwrap();
    assert_eq!(gateway.address, address1.to_vec());
    assert_eq!(gateway.device_type(), DeviceType::WifiIndoor);
    assert_eq!(
        u64::from_str_radix(&gateway.metadata.clone().unwrap().location, 16).unwrap(),
        loc1
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_info_batch_v1(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: updated_at,
        refreshed_at: updated_at,
        last_changed_at: updated_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(updated_at),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: created_at,
        updated_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v1(
        &mut client,
        &vec![address1.clone(), address2.clone()],
        &admin_key,
    )
    .await?;
    let res = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV1>>()
        .await;

    let gateways = res.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 2);

    let gw1 = gateways
        .iter()
        .find(|v| v.address == address1.to_vec())
        .unwrap();

    assert_eq!(
        u64::from_str_radix(&gw1.metadata.clone().unwrap().location, 16).unwrap(),
        loc1
    );

    let gw2 = gateways
        .iter()
        .find(|v| v.address == address2.to_vec())
        .unwrap();
    assert_eq!(
        u64::from_str_radix(&gw2.metadata.clone().unwrap().location, 16).unwrap(),
        loc2
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_info_batch_v2(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let updated_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: updated_at,
        refreshed_at: updated_at,
        last_changed_at: updated_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(updated_at),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: created_at,
        updated_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v2(
        &mut client,
        &vec![address1.clone(), address2.clone()],
        &admin_key,
    )
    .await?;
    let res = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;

    let gateways = res.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 2);

    let gw1 = gateways
        .iter()
        .find(|v| v.address == address1.to_vec())
        .unwrap();

    assert_eq!(
        u64::from_str_radix(&gw1.metadata.clone().unwrap().location, 16).unwrap(),
        loc1
    );

    let deployment_info = gw1.metadata.clone().unwrap().deployment_info.unwrap();

    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
            assert_eq!(v.electrical_down_tilt, 0);
            assert_eq!(v.mechanical_down_tilt, 0);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    let gw2 = gateways
        .iter()
        .find(|v| v.address == address2.to_vec())
        .unwrap();
    assert_eq!(
        u64::from_str_radix(&gw2.metadata.clone().unwrap().location, 16).unwrap(),
        loc2
    );

    println!("{:?}", gw2.metadata);
    assert!(gw2.metadata.clone().unwrap().deployment_info.is_none());

    Ok(())
}

#[sqlx::test]
async fn gateway_info_batch_v2_updated_at_check(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let address3 = make_keypair().public_key().clone();
    let loc3 = 631711286145006591_u64;

    let address4 = make_keypair().public_key().clone();
    let loc4 = 0x8c44a82aed527ff_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);
    let updated_at = Utc::now() - Duration::hours(4);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: refreshed_at,
        refreshed_at: refreshed_at,
        last_changed_at: refreshed_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(refreshed_at),
        location_asserts: Some(1),
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
    };
    gateway2.insert(&pool).await?;

    let gateway3 = Gateway {
        address: address3.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: created_at,
        updated_at: updated_at,
        refreshed_at: refreshed_at,
        last_changed_at: updated_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc3),
        location_changed_at: Some(updated_at),
        location_asserts: Some(1),
    };
    gateway3.insert(&pool).await?;

    let gateway4 = Gateway {
        address: address4.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: created_at,
        updated_at: updated_at,
        refreshed_at: created_at,
        last_changed_at: updated_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc4),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
    };
    gateway4.insert(&pool).await?;

    let (addr, _handle) =
        common::spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v2(
        &mut client,
        &vec![
            address1.clone(),
            address2.clone(),
            address3.clone(),
            make_keypair().public_key().clone(), // it doesn't exist
        ],
        &admin_key,
    )
    .await?;
    let resp = stream
        .filter_map(|result| async { result.ok() })
        .collect::<Vec<GatewayInfoStreamResV2>>()
        .await;
    let gateways = resp.first().unwrap().gateways.clone();
    assert_eq!(gateways.len(), 3);
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == address1.to_vec())
            .unwrap()
            .updated_at,
        refreshed_at.timestamp() as u64
    );
    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == address2.to_vec())
            .unwrap()
            .updated_at,
        created_at.timestamp() as u64
    );

    assert_eq!(
        gateways
            .iter()
            .find(|v| v.address == address3.to_vec())
            .unwrap()
            .updated_at,
        updated_at.timestamp() as u64
    );

    Ok(())
}

// #[sqlx::test]
// async fn gateway_info_v2_no_mobile_tracker_record(pool: PgPool) {
//     let admin_key = make_keypair();
//     let asset1_pubkey = make_keypair().public_key().clone();
//     let asset1_hex_idx = 631711281837647359_i64;
//     let asset2_pubkey = make_keypair().public_key().clone();
//     let asset2_hex_idx = 631711286145955327_i64;

//     let created_at = Utc::now() - Duration::hours(5);
//     let refreshed_at = Utc::now() - Duration::hours(3);

//     create_metadata_db_tables(&pool).await;
//     add_db_record(
//         &pool,
//         "asset1",
//         Some(asset1_hex_idx),
//         "\"wifiIndoor\"",
//         asset1_pubkey.clone().into(),
//         created_at,
//         Some(refreshed_at),
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
//     )
//     .await;

//     add_db_record(
//         &pool,
//         "asset2",
//         Some(asset2_hex_idx),
//         "\"wifiIndoor\"",
//         asset2_pubkey.clone().into(),
//         created_at,
//         None,
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
//     )
//     .await;

//     let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
//     let mut client = GatewayClient::connect(addr).await?;

//     // asset 1
//     let req = make_signed_info_request(&asset1_pubkey, &admin_key);
//     let resp = client.info_v2(req).await.unwrap().into_inner();
//     let gw_info = resp.info.unwrap();
//     let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
//     assert_eq!(pub_key, asset1_pubkey.clone());
//     assert_eq!(gw_info.updated_at, refreshed_at.timestamp() as u64);

//     // asset 2
//     let req = make_signed_info_request(&asset2_pubkey, &admin_key);
//     let resp = client.info_v2(req).await.unwrap().into_inner();
//     let gw_info = resp.info.unwrap();
//     let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
//     assert_eq!(pub_key, asset2_pubkey.clone());
//     assert_eq!(gw_info.updated_at, created_at.timestamp() as u64);
// }

// #[sqlx::test]
// async fn gateway_info_v2(pool: PgPool) {
//     let admin_key = make_keypair();
//     let asset1_pubkey = make_keypair().public_key().clone();
//     let asset1_hex_idx = 631711281837647359_i64;
//     let asset2_pubkey = make_keypair().public_key().clone();
//     let created_at = Utc::now() - Duration::hours(5);
//     let updated_at = Utc::now() - Duration::hours(3);

//     create_metadata_db_tables(&pool).await;
//     add_db_record(
//         &pool,
//         "asset1",
//         Some(asset1_hex_idx),
//         "\"wifiIndoor\"",
//         asset1_pubkey.clone().into(),
//         created_at,
//         Some(updated_at),
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
//     )
//     .await;
//     add_mobile_tracker_record(&pool, asset1_pubkey.clone().into(), updated_at, None, None).await;

//     let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
//     let mut client = GatewayClient::connect(addr).await?;

//     let req = make_signed_info_request(&asset1_pubkey, &admin_key);
//     let resp = client.info_v2(req).await.unwrap().into_inner();

//     let gw_info = resp.info.unwrap();
//     let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
//     assert_eq!(pub_key, asset1_pubkey.clone());
//     assert_eq!(
//         DeviceType::try_from(gw_info.device_type).unwrap(),
//         DeviceType::WifiIndoor
//     );
//     assert_eq!(gw_info.updated_at, updated_at.timestamp() as u64);
//     assert_eq!(
//         i64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
//         asset1_hex_idx
//     );

//     let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();

//     match deployment_info {
//         DeploymentInfo::WifiDeploymentInfo(v) => {
//             assert_eq!(v.antenna, 18);
//             assert_eq!(v.azimuth, 161);
//             assert_eq!(v.elevation, 2);
//             assert_eq!(v.electrical_down_tilt, 3);
//             assert_eq!(v.mechanical_down_tilt, 4);
//         }
//         DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
//     };

//     // Non-existent
//     let req = make_signed_info_request(&asset2_pubkey, &admin_key);
//     let resp_err = client
//         .info_v2(req)
//         .await
//         .expect_err("testing expects error");

//     assert_eq!(resp_err.code(), Code::NotFound);
// }

// #[sqlx::test]
// async fn gateway_info_stream_v2_updated_at_check(pool: PgPool) {
//     let admin_key = make_keypair();
//     let asset1_pubkey = make_keypair().public_key().clone();
//     let asset1_hex_idx = 631711281837647359_i64;
//     let asset2_pubkey = make_keypair().public_key().clone();
//     let asset2_hex_idx = 631711286145955327_i64;
//     let asset3_hex_idx = 631711286145006591_i64;
//     let asset3_pubkey = make_keypair().public_key().clone();

//     let created_at = Utc::now() - Duration::hours(5);
//     let refreshed_at = Utc::now() - Duration::hours(3);
//     let updated_at = Utc::now() - Duration::hours(4);

//     create_metadata_db_tables(&pool).await;
//     add_db_record(
//         &pool,
//         "asset1",
//         Some(asset1_hex_idx),
//         "\"wifiIndoor\"",
//         asset1_pubkey.clone().into(),
//         created_at,
//         Some(refreshed_at),
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
//     )
//     .await;

//     add_db_record(
//         &pool,
//         "asset2",
//         Some(asset2_hex_idx),
//         "\"wifiIndoor\"",
//         asset2_pubkey.clone().into(),
//         created_at,
//         None,
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 161, "elevation": 2, "electricalDownTilt": 3, "mechanicalDownTilt": 4}}"#)
//     )
//     .await;

//     add_db_record(
//         &pool,
//         "asset3",
//         Some(asset3_hex_idx),
//         "\"wifiDataOnly\"",
//         asset3_pubkey.clone().into(),
//         created_at,
//         Some(refreshed_at),
//         None,
//     )
//     .await;
//     add_mobile_tracker_record(&pool, asset3_pubkey.clone().into(), updated_at, None, None).await;

//     let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
//     let mut client = GatewayClient::connect(addr).await?;

//     let req = make_gateway_stream_signed_req_v2(&admin_key, &[], 0);
//     let stream = client.info_stream_v2(req).await.unwrap().into_inner();

//     let resp = stream
//         .filter_map(|result| async { result.ok() })
//         .collect::<Vec<GatewayInfoStreamResV2>>()
//         .await;
//     let gateways = resp.first().unwrap().gateways.clone();
//     assert_eq!(gateways.len(), 3);
//     assert_eq!(
//         gateways
//             .iter()
//             .find(|v| v.address == asset1_pubkey.to_vec())
//             .unwrap()
//             .updated_at,
//         refreshed_at.timestamp() as u64
//     );
//     assert_eq!(
//         gateways
//             .iter()
//             .find(|v| v.address == asset2_pubkey.to_vec())
//             .unwrap()
//             .updated_at,
//         created_at.timestamp() as u64
//     );

//     assert_eq!(
//         gateways
//             .iter()
//             .find(|v| v.address == asset3_pubkey.to_vec())
//             .unwrap()
//             .updated_at,
//         updated_at.timestamp() as u64
//     );
// }

// #[sqlx::test]
// async fn gateway_stream_info_v2_deployment_info(pool: PgPool) {
//     let admin_key = make_keypair();
//     let asset1_pubkey = make_keypair().public_key().clone();
//     let asset1_hex_idx = 631711281837647359_i64;
//     let asset2_hex_idx = 631711286145955327_i64;
//     let asset3_hex_idx = 631711286145006591_i64;
//     let asset2_pubkey = make_keypair().public_key().clone();
//     let asset3_pubkey = make_keypair().public_key().clone();
//     let now = Utc::now();

//     create_metadata_db_tables(&pool).await;
//     add_db_record(
//         &pool,
//         "asset1",
//         Some(asset1_hex_idx),
//         "\"wifiIndoor\"",
//         asset1_pubkey.clone().into(),
//         now,
//         Some(now),
//         Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
//     )
//     .await;
//     add_db_record(
//         &pool,
//         "asset2",
//         Some(asset2_hex_idx),
//         "\"wifiDataOnly\"",
//         asset2_pubkey.clone().into(),
//         now,
//         Some(now),
//         // Should be returned None in deployment info
//         Some(r#"{"wifiInfoV0Invalid": {"antenna": 18}}"#),
//     )
//     .await;
//     add_db_record(
//         &pool,
//         "asset3",
//         Some(asset3_hex_idx),
//         "\"wifiDataOnly\"",
//         asset3_pubkey.clone().into(),
//         now,
//         Some(now),
//         None,
//     )
//     .await;

//     let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;

//     let mut client = GatewayClient::connect(addr).await?;

//     // Check wifi indoor
//     let req = make_gateway_stream_signed_req_v2(&admin_key, &[DeviceType::WifiIndoor], 0);
//     let mut stream = client.info_stream_v2(req).await.unwrap().into_inner();
//     let res = stream.next().await.unwrap().unwrap();
//     let gw_info = res.gateways.first().unwrap();
//     let pub_key = PublicKey::from_bytes(gw_info.address.clone()).unwrap();
//     assert_eq!(pub_key, asset1_pubkey.clone());
//     assert_eq!(
//         DeviceType::try_from(gw_info.device_type).unwrap(),
//         DeviceType::WifiIndoor
//     );
//     assert_eq!(
//         i64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
//         asset1_hex_idx
//     );
//     assert!(stream.next().await.is_none());

//     // Check wifi data only
//     let req = make_gateway_stream_signed_req_v2(&admin_key, &[DeviceType::WifiDataOnly], 0);
//     let stream = client.info_stream_v2(req).await.unwrap().into_inner();

//     let resp = stream
//         .filter_map(|result| async { result.ok() })
//         .collect::<Vec<GatewayInfoStreamResV2>>()
//         .await;
//     let gateways = resp.first().unwrap().gateways.clone();
//     assert_eq!(gateways.len(), 2);
//     let device_type = gateways.first().unwrap().device_type;
//     assert_eq!(
//         DeviceType::try_from(device_type).unwrap(),
//         DeviceType::WifiDataOnly
//     );
//     let device_type = gateways.get(1).unwrap().device_type;
//     assert_eq!(
//         DeviceType::try_from(device_type).unwrap(),
//         DeviceType::WifiDataOnly
//     );

//     // Check all
//     let req = make_gateway_stream_signed_req_v2(&admin_key, &[], 0);
//     let stream = client.info_stream_v2(req).await.unwrap().into_inner();

//     let resp = stream
//         .filter_map(|result| async { result.ok() })
//         .collect::<Vec<GatewayInfoStreamResV2>>()
//         .await;
//     let gateways = resp.first().unwrap().gateways.clone();

//     // Check deployment info
//     assert_eq!(gateways.len(), 3);
//     for gw in gateways {
//         if let Some(metadata) = &gw.metadata {
//             if DeviceType::try_from(gw.device_type).unwrap() != DeviceType::WifiIndoor {
//                 assert!(metadata.deployment_info.is_none());
//             } else {
//                 let deployment_info = metadata.deployment_info.as_ref().unwrap();
//                 match deployment_info {
//                     DeploymentInfo::WifiDeploymentInfo(v) => {
//                         assert_eq!(v.antenna, 18);
//                         assert_eq!(v.azimuth, 160);
//                         assert_eq!(v.elevation, 5);
//                         assert_eq!(v.electrical_down_tilt, 1);
//                         assert_eq!(v.mechanical_down_tilt, 2);
//                     }
//                     DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
//                 };
//             }
//         }
//     }
// }

fn make_signed_info_request(address: &PublicKey, signer: &Keypair) -> proto::GatewayInfoReqV1 {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}

async fn gateway_info_stream_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    device_types: &[DeviceType],
) -> anyhow::Result<proto::GatewayInfoStreamResV1> {
    let mut req = GatewayInfoStreamReqV1 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
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

async fn gateway_info_stream_v2(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    device_types: &[DeviceType],
    min_updated_at: u64,
) -> anyhow::Result<proto::GatewayInfoStreamResV2> {
    let mut req = GatewayInfoStreamReqV2 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
        min_updated_at,
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

async fn info_batch_v1(
    client: &mut GatewayClient<tonic::transport::Channel>,
    addresses: &[PublicKey],
    signer: &Keypair,
) -> anyhow::Result<tonic::Streaming<proto::GatewayInfoStreamResV1>> {
    let mut req = proto::GatewayInfoBatchReqV1 {
        addresses: addresses.iter().map(|v| v.to_vec()).collect(),
        batch_size: 42,
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    let stream = client.info_batch(req).await?.into_inner();

    Ok(stream)
}

async fn info_batch_v2(
    client: &mut GatewayClient<tonic::transport::Channel>,
    addresses: &[PublicKey],
    signer: &Keypair,
) -> anyhow::Result<tonic::Streaming<proto::GatewayInfoStreamResV2>> {
    let mut req = proto::GatewayInfoBatchReqV1 {
        addresses: addresses.iter().map(|v| v.to_vec()).collect(),
        batch_size: 42,
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    let stream = client.info_batch_v2(req).await?.into_inner();

    Ok(stream)
}
