use crate::common::{
    gateway_metadata_db::{self, create_tables},
    make_keypair, spawn_gateway_service,
};
use chrono::{DateTime, Utc};
use h3o::{LatLng, Resolution};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{self as proto, GatewayClient},
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

#[sqlx::test]
async fn gateway_info_authorization_errors(pool: PgPool) -> anyhow::Result<()> {
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
async fn gateway_info(pool: PgPool) -> anyhow::Result<()> {
    let admin_key = make_keypair();
    let pub_key = make_keypair().public_key().clone();
    let now = Utc::now();

    create_tables(&pool).await?;

    let gateway = insert_gateway(&pool, now, pub_key.clone().into()).await?;

    let (addr, _) = spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await?;

    let mut client = GatewayClient::connect(addr).await?;

    let res = gateway_info_v1(&mut client, &pub_key, &admin_key).await?;

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

fn make_signed_info_request(address: &PublicKey, signer: &Keypair) -> proto::GatewayInfoReqV1 {
    let mut req = proto::GatewayInfoReqV1 {
        address: address.to_vec(),
        signer: signer.public_key().to_vec(),
        signature: vec![],
    };
    req.signature = signer.sign(&req.encode_to_vec()).unwrap();
    req
}

async fn gateway_info_v1(
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

    // Insert test data into iot_hotspot_infos
    gateway_metadata_db::insert_gateway(
        pool,
        "address1",                                 // address (PRIMARY KEY)
        "asset1",                                   // asset
        Some(gateway.location.unwrap() as i64),     // location
        gateway.elevation.map(|v| v as i64),        // elevation
        gateway.gain.map(|v| v as i64),             // gain
        gateway.is_full_hotspot,                    // is_full_hotspot
        gateway.location_asserts.map(|v| v as i32), // num_location_asserts
        gateway.is_active,                          // is_active
        Some(0),                                    // dc_onboarding_fee_paid
        gateway.created_at,                         // created_at
        gateway.refreshed_at,                       // refreshed_at
        Some(0),                                    // last_block
        gateway.address.clone(),                    // key (PublicKeyBinary)
    )
    .await?;

    let loc_bytes = h3_index.to_le_bytes();
    let mut encoder = Encoder::new(Vec::new())?;
    encoder.write_all(&loc_bytes)?;
    let compressed = encoder.finish().into_result()?;

    region_map::update_region(
        Region::Us915,
        &BlockchainRegionParamsV1 {
            region_params: vec![],
        },
        Some(&compressed),
        pool,
    )
    .await?;

    Ok(gateway)
}
