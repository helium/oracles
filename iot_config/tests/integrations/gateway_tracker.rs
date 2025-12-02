use crate::common::{
    self,
    gateway_metadata_db::{
        create_tables, insert_gateway, insert_gateway_with_invalid_key, insert_iot_hotspot_infos,
    },
    make_keypair,
};
use chrono::Utc;
use iot_config::gateway::{db::Gateway, tracker};
use sqlx::PgPool;

#[sqlx::test]
async fn execute_test(pool: PgPool) -> anyhow::Result<()> {
    let pubkey1 = make_keypair().public_key().clone();
    let location = 631_711_281_837_647_359_i64;
    let now = Utc::now() - chrono::Duration::seconds(10);

    // Ensure tables exist
    create_tables(&pool).await?;

    // Insert test data into iot_hotspot_infos
    insert_gateway(
        &pool,
        "address1",             // address (PRIMARY KEY)
        "asset1",               // asset
        Some(location),         // location
        Some(1),                // elevation
        Some(2),                // gain
        Some(true),             // is_full_hotspot
        Some(3),                // num_location_asserts
        Some(true),             // is_active
        Some(0),                // dc_onboarding_fee_paid
        now,                    // created_at
        Some(now),              // refreshed_at
        Some(0),                // last_block
        pubkey1.clone().into(), // key (PublicKeyBinary)
    )
    .await?;

    // Execute tracker logic
    tracker::execute(&pool, &pool).await?;

    // Retrieve gateway record and assert fields
    let gateway = Gateway::get_by_address(&pool, &pubkey1.clone().into())
        .await?
        .expect("gateway not found");

    assert_eq!(gateway.created_at, common::nanos_trunc(now));
    assert_eq!(gateway.elevation, Some(1));
    assert_eq!(gateway.gain, Some(2));
    assert_eq!(gateway.is_active, Some(true));
    assert_eq!(gateway.is_full_hotspot, Some(true));
    assert!(gateway.last_changed_at > now);
    assert_eq!(gateway.location, Some(location as u64));
    assert_eq!(gateway.location_asserts, Some(3));
    assert!(gateway.location_changed_at.is_some());
    assert_eq!(
        gateway.location_changed_at.unwrap(),
        common::nanos_trunc(now)
    );
    assert!(gateway.refreshed_at.is_some());
    assert_eq!(gateway.refreshed_at.unwrap(), common::nanos_trunc(now));
    assert!(gateway.updated_at > now);

    let refreshed_at = Utc::now();
    let location = 666_711_281_837_647_360_i64;
    // Insert test data into iot_hotspot_infos
    insert_iot_hotspot_infos(
        &pool,
        "address1",         // address (PRIMARY KEY)
        "asset1",           // asset
        Some(location),     // location
        Some(10),           // elevation
        Some(20),           // gain
        Some(false),        // is_full_hotspot
        Some(30),           // num_location_asserts
        Some(false),        // is_active
        Some(0),            // dc_onboarding_fee_paid
        now,                // created_at
        Some(refreshed_at), // refreshed_at
        Some(0),            // last_block
    )
    .await?;

    // Execute tracker logic
    tracker::execute(&pool, &pool).await?;

    // Retrieve gateway record and assert fields
    let gateway = Gateway::get_by_address(&pool, &pubkey1.clone().into())
        .await?
        .expect("gateway not found");

    assert_eq!(gateway.created_at, common::nanos_trunc(now));
    assert_eq!(gateway.elevation, Some(10));
    assert_eq!(gateway.gain, Some(20));
    assert_eq!(gateway.is_active, Some(false));
    assert_eq!(gateway.is_full_hotspot, Some(false));
    assert_eq!(gateway.last_changed_at, common::nanos_trunc(refreshed_at));
    assert_eq!(gateway.location, Some(location as u64));
    assert_eq!(gateway.location_asserts, Some(30));
    assert!(gateway.location_changed_at.is_some());
    assert_eq!(
        gateway.location_changed_at.unwrap(),
        common::nanos_trunc(refreshed_at)
    );
    assert!(gateway.refreshed_at.is_some());
    assert_eq!(
        gateway.refreshed_at.unwrap(),
        common::nanos_trunc(refreshed_at)
    );
    assert!(gateway.updated_at > refreshed_at);

    Ok(())
}

#[sqlx::test]
async fn execute_test_with_invalid_entity_key(pool: PgPool) -> anyhow::Result<()> {
    let pubkey1 = make_keypair().public_key().clone();
    let location = 631_711_281_837_647_359_i64;
    let now = Utc::now() - chrono::Duration::seconds(10);

    // Ensure tables exist
    create_tables(&pool).await?;

    // Insert a valid gateway
    insert_gateway(
        &pool,
        "valid_address",        // address (PRIMARY KEY)
        "valid_asset",          // asset
        Some(location),         // location
        Some(1),                // elevation
        Some(2),                // gain
        Some(true),             // is_full_hotspot
        Some(3),                // num_location_asserts
        Some(true),             // is_active
        Some(0),                // dc_onboarding_fee_paid
        now,                    // created_at
        Some(now),              // refreshed_at
        Some(0),                // last_block
        pubkey1.clone().into(), // key (PublicKeyBinary)
    )
    .await?;

    // Insert a gateway with an invalid entity_key (like the real-world failures we found)
    // This uses bytes that start with 0x00 which fail helium-crypto validation
    let invalid_key_bytes =
        hex::decode("00d34decd6cdfed91784d98d7525fb8a3c1ee381d7052bfcb3d1c90b3f54b09fc9").unwrap();

    insert_gateway_with_invalid_key(
        &pool,
        "invalid_address", // address (PRIMARY KEY)
        "invalid_asset",   // asset
        Some(location),    // location
        Some(5),           // elevation
        Some(6),           // gain
        Some(false),       // is_full_hotspot
        Some(7),           // num_location_asserts
        Some(false),       // is_active
        Some(0),           // dc_onboarding_fee_paid
        now,               // created_at
        Some(now),         // refreshed_at
        Some(0),           // last_block
        invalid_key_bytes, // invalid entity_key bytes
    )
    .await?;

    // Execute tracker logic - should process valid gateway and skip invalid one
    tracker::execute(&pool, &pool).await?;

    // Verify that the valid gateway was inserted
    let gateway = Gateway::get_by_address(&pool, &pubkey1.clone().into())
        .await?
        .expect("valid gateway should be found");

    assert_eq!(gateway.created_at, common::nanos_trunc(now));
    assert_eq!(gateway.elevation, Some(1));
    assert_eq!(gateway.gain, Some(2));
    assert_eq!(gateway.is_active, Some(true));
    assert_eq!(gateway.is_full_hotspot, Some(true));
    assert_eq!(gateway.location, Some(location as u64));
    assert_eq!(gateway.location_asserts, Some(3));

    // Verify that there's only 1 gateway in the database (the invalid one was skipped)
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM gateways")
        .fetch_one(&pool)
        .await?;

    assert_eq!(count, 1, "Only the valid gateway should be inserted");

    Ok(())
}
