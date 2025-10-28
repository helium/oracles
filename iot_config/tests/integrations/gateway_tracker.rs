use crate::common::{
    self,
    gateway_metadata_db::{create_tables, insert_gateway, insert_iot_hotspot_infos},
    make_keypair,
};
use chrono::Utc;
use custom_tracing::Settings;
use iot_config::gateway::{db::Gateway, tracker};
use sqlx::PgPool;

#[sqlx::test]
async fn execute_test(pool: PgPool) -> anyhow::Result<()> {
    custom_tracing::init("iot_config=debug,info".to_string(), Settings::default()).await?;

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
