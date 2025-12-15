use crate::common::{make_keypair, spawn_gateway_service};
use chrono::{DateTime, Duration, Utc};
use futures::stream::StreamExt;
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::services::mobile_config::{
    self as proto, gateway_metadata_v2::DeploymentInfo, DeviceType, GatewayClient,
    GatewayInfoStreamReqV1, GatewayInfoStreamReqV2, GatewayInfoStreamResV1, GatewayInfoStreamResV2,
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
    // this test. But we're only interested in Authorization Errors.

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
    let gws = GatewayService::new(key_cache, pool.clone(), Arc::new(server_key));
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
        inserted_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(now),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: now_plus_10,
        inserted_at: now_plus_10,
        refreshed_at: now_plus_10,
        last_changed_at: now_plus_10,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(now_plus_10),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
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
async fn gateway_stream_info_v2_by_type(pool: PgPool) -> anyhow::Result<()> {
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
        inserted_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(now),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at: now_plus_10,
        inserted_at: now_plus_10,
        refreshed_at: now_plus_10,
        last_changed_at: now_plus_10,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(now_plus_10),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
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
async fn gateway_stream_info_v2(pool: PgPool) -> anyhow::Result<()> {
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
    let inserted_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at,
        refreshed_at: inserted_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc1),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at,
        refreshed_at: inserted_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(1),
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let gateway3 = Gateway {
        address: address3.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at,
        inserted_at,
        refreshed_at: inserted_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(1),
        elevation: Some(2),
        azimuth: Some(3),
        location: Some(loc3),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway3.insert(&pool).await?;

    let gateway4 = Gateway {
        address: address4.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc4),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway4.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let res = gateway_info_stream_v2(&mut client, &admin_key, &[], inserted_at.timestamp() as u64)
        .await?;
    assert_eq!(res.gateways.len(), 3);

    let gateways = res.gateways;

    let gw1 = gateways
        .iter()
        .find(|v| v.address == address1.to_vec())
        .unwrap();
    assert_eq!(gw1.device_type(), DeviceType::WifiIndoor);
    assert_eq!(
        u64::from_str_radix(&gw1.metadata.clone().unwrap().location, 16).unwrap(),
        loc1
    );
    assert_eq!(gw1.updated_at, inserted_at.timestamp() as u64);
    assert_eq!(gw1.metadata.clone().unwrap().deployment_info, None);

    let gw2 = gateways
        .iter()
        .find(|v| v.address == address2.to_vec())
        .unwrap();
    assert_eq!(gw2.device_type(), DeviceType::WifiIndoor);
    assert_eq!(
        u64::from_str_radix(&gw2.metadata.clone().unwrap().location, 16).unwrap(),
        loc2
    );
    assert_eq!(gw2.updated_at, inserted_at.timestamp() as u64);
    let deployment_info = gw2.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 1);
            assert_eq!(v.elevation, 0);
            assert_eq!(v.azimuth, 0);
            assert_eq!(v.electrical_down_tilt, 0);
            assert_eq!(v.mechanical_down_tilt, 0);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    let gw3 = gateways
        .iter()
        .find(|v| v.address == address3.to_vec())
        .unwrap();
    assert_eq!(gw3.device_type(), DeviceType::WifiDataOnly);
    assert_eq!(
        u64::from_str_radix(&gw3.metadata.clone().unwrap().location, 16).unwrap(),
        loc3
    );
    assert_eq!(gw3.updated_at, inserted_at.timestamp() as u64);
    let deployment_info = gw3.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 1);
            assert_eq!(v.elevation, 2);
            assert_eq!(v.azimuth, 3);
            assert_eq!(v.electrical_down_tilt, 0);
            assert_eq!(v.mechanical_down_tilt, 0);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    let gw4 = gateways.iter().find(|v| v.address == address4.to_vec());
    assert_eq!(gw4, None);

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
    let inserted_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at,
        refreshed_at: inserted_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v1(
        &mut client,
        &[address1.clone(), address2.clone()],
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
    let inserted_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at,
        refreshed_at: inserted_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v2(
        &mut client,
        &[address1.clone(), address2.clone()],
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
    let inserted_at = Utc::now() - Duration::hours(4);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: refreshed_at,
        refreshed_at,
        last_changed_at: refreshed_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(refreshed_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let gateway3 = Gateway {
        address: address3.clone().into(),
        gateway_type: GatewayType::WifiDataOnly,
        created_at,
        inserted_at,
        refreshed_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc3),
        location_changed_at: Some(inserted_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway3.insert(&pool).await?;

    let gateway4 = Gateway {
        address: address4.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at,
        refreshed_at: created_at,
        last_changed_at: inserted_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc4),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway4.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let stream = info_batch_v2(
        &mut client,
        &[
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
        inserted_at.timestamp() as u64
    );

    Ok(())
}

#[sqlx::test]
async fn gateway_info_v2(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address1 = make_keypair().public_key().clone();
    let loc1 = 631711281837647359_u64;

    let address2 = make_keypair().public_key().clone();
    let loc2 = 631711286145955327_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);

    let gateway1 = Gateway {
        address: address1.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: refreshed_at,
        refreshed_at,
        last_changed_at: refreshed_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc1),
        location_changed_at: Some(refreshed_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway1.insert(&pool).await?;

    let gateway2 = Gateway {
        address: address2.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc2),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway2.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    let res = info_request_v2(&mut client, &address1, &admin_key).await?;
    let gw_info = res.info.unwrap();
    assert_eq!(gw_info.address, address1.to_vec());
    assert_eq!(gw_info.updated_at, refreshed_at.timestamp() as u64);
    assert_eq!(
        u64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        loc1
    );
    let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    let res = info_request_v2(&mut client, &address2, &admin_key).await?;
    let gw_info = res.info.unwrap();
    assert_eq!(gw_info.address, address2.to_vec());
    assert_eq!(gw_info.updated_at, created_at.timestamp() as u64);
    assert_eq!(
        u64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        loc2
    );
    let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    // Non-existent
    let address3 = make_keypair().public_key().clone();
    let req = make_signed_info_request(&address3, &admin_key);
    let resp_err = client
        .info_v2(req)
        .await
        .expect_err("testing expects error");

    assert_eq!(resp_err.code(), Code::NotFound);

    Ok(())
}

#[sqlx::test]
async fn gateway_info_at_timestamp(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();

    let address = make_keypair().public_key().clone();
    let loc_original = 631711281837647359_u64;
    let loc_recent = 631711281837647358_u64;

    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);

    let gateway_original = Gateway {
        address: address.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: refreshed_at,
        refreshed_at,
        last_changed_at: refreshed_at,
        hash: "".to_string(),
        antenna: Some(10),
        elevation: Some(4),
        azimuth: Some(168),
        location: Some(loc_original),
        location_changed_at: Some(refreshed_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway_original.insert(&pool).await?;

    let pubkey = address.clone().into();

    // Change original gateway's inserted_at value to 10 minutes ago
    let new_inserted_at = Utc::now() - Duration::minutes(10);
    update_gateway_inserted_at(&pool, &pubkey, &new_inserted_at).await?;

    let query_time_original = Utc::now();

    let gateway_recent = Gateway {
        address: address.clone().into(),
        gateway_type: GatewayType::WifiIndoor,
        created_at,
        inserted_at: created_at,
        refreshed_at: created_at,
        last_changed_at: created_at,
        hash: "".to_string(),
        antenna: Some(18),
        elevation: Some(2),
        azimuth: Some(161),
        location: Some(loc_recent),
        location_changed_at: Some(created_at),
        location_asserts: Some(1),
        owner: None,
        hash_v2: None,
    };
    gateway_recent.insert(&pool).await?;

    let (addr, _handle) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await?;

    // Get most recent gateway info
    let query_time_recent = Utc::now() + Duration::minutes(10);
    let res =
        info_at_timestamp_request(&mut client, &address, &admin_key, &query_time_recent).await;

    // Assert that recent gateway was returned
    let gw_info = res?.info.unwrap();
    assert_eq!(gw_info.address, address.to_vec());
    let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 18);
            assert_eq!(v.azimuth, 161);
            assert_eq!(v.elevation, 2);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };
    assert_eq!(
        u64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        loc_recent
    );

    // Get original gateway info by using an earlier inserted_at condition
    let res =
        info_at_timestamp_request(&mut client, &address, &admin_key, &query_time_original).await;

    // Assert that original gateway was returned
    let gw_info = res?.info.unwrap();
    assert_eq!(gw_info.address, address.to_vec());
    let deployment_info = gw_info.metadata.clone().unwrap().deployment_info.unwrap();
    match deployment_info {
        DeploymentInfo::WifiDeploymentInfo(v) => {
            assert_eq!(v.antenna, 10);
            assert_eq!(v.azimuth, 168);
            assert_eq!(v.elevation, 4);
        }
        DeploymentInfo::CbrsDeploymentInfo(_) => panic!(),
    };

    assert_eq!(
        u64::from_str_radix(&gw_info.metadata.clone().unwrap().location, 16).unwrap(),
        loc_original
    );

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

async fn info_request_v2(
    client: &mut GatewayClient<tonic::transport::Channel>,
    address: &PublicKey,
    signer: &Keypair,
) -> anyhow::Result<proto::GatewayInfoResV2> {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    let res = client.info_v2(req).await?.into_inner();
    Ok(res)
}

async fn info_at_timestamp_request(
    client: &mut GatewayClient<tonic::transport::Channel>,
    address: &PublicKey,
    signer: &Keypair,
    query_time: &DateTime<Utc>,
) -> anyhow::Result<proto::GatewayInfoResV2> {
    let mut req = proto::GatewayInfoAtTimestampReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
        query_time: query_time.timestamp() as u64,
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    let res = client.info_at_timestamp(req).await?.into_inner();
    Ok(res)
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
    #[allow(deprecated)]
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
    min_inserted_at: u64,
) -> anyhow::Result<proto::GatewayInfoStreamResV2> {
    let mut req = GatewayInfoStreamReqV2 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
        min_updated_at: min_inserted_at,
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
    #[allow(deprecated)]
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

async fn update_gateway_inserted_at(
    pool: &PgPool,
    address: &PublicKeyBinary,
    new_inserted_at: &DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE gateways
        SET inserted_at = $1
        WHERE address = $2;
        "#,
    )
    .bind(new_inserted_at)
    .bind(address.as_ref())
    .execute(pool)
    .await?;

    Ok(())
}
