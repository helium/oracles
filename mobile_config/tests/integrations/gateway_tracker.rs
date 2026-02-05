use crate::common::{gateway_metadata_db, make_keypair};
use chrono::{Timelike, Utc};
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    tracker,
};
use rand::{seq::SliceRandom, thread_rng};
use sqlx::PgPool;

#[sqlx::test]
async fn test_gateway_tracker_updates_changed_gateways(pool: PgPool) -> anyhow::Result<()> {
    const TOTAL: usize = 2_000;

    let now = Utc::now()
        .with_nanosecond(Utc::now().timestamp_subsec_micros() * 1000)
        .unwrap();

    // ensure tables exist
    gateway_metadata_db::create_tables(&pool).await;

    // Prepare the bulk insert data
    let gateways: Vec<gateway_metadata_db::GatewayInsert> = (0..TOTAL)
        .map(|i| {
            let pubkey = make_keypair().public_key().clone();
            let hex_val = 631_711_281_837_647_359_i64 + i as i64;

            gateway_metadata_db::GatewayInsert {
                asset: format!("asset{}", i),
                location: Some(hex_val),
                device_type: "\"wifiIndoor\"".to_string(),
                key: pubkey.into(),
                created_at: now,
                refreshed_at: Some(now),
                deployment_info: None,
            }
        })
        .collect();

    // Bulk insert all gateways in one shot
    gateway_metadata_db::insert_gateway_bulk(&pool, &gateways, 1000).await?;

    tracing::info!("inserted {} gateways, running tracker", TOTAL);

    // now run the tracker execute function
    tracker::execute(&pool, &pool).await?;

    // Check that we have TOTAL gateways in the DB
    let total = count_gateways(&pool).await?;
    assert_eq!(TOTAL as i64, total);

    // Sample 100 gateways to verify
    let mut rng = thread_rng();
    let sample_size = 100;
    let sample: Vec<_> = gateways.choose_multiple(&mut rng, sample_size).collect();

    let new_loc = 0_i64;
    let now = Utc::now()
        .with_nanosecond(Utc::now().timestamp_subsec_micros() * 1000)
        .unwrap();

    for gw_insert in sample.clone() {
        let gateway = Gateway::get_by_address(&pool, &gw_insert.key)
            .await?
            .expect("gateway not found");

        assert_eq!(gateway.address, gw_insert.key.clone());
        assert_eq!(gateway.hash_params.gateway_type, GatewayType::WifiIndoor);
        assert_eq!(gateway.created_at, gw_insert.created_at);
        assert_eq!(Some(gateway.last_changed_at), gw_insert.refreshed_at);
        assert_eq!(gateway.hash_params.antenna, None);
        assert_eq!(gateway.hash_params.elevation, None);
        assert_eq!(gateway.hash_params.azimuth, None);
        assert_eq!(
            gateway.hash_params.location,
            gw_insert.location.map(|v| v as u64)
        );
        assert_eq!(gateway.location_changed_at, gw_insert.refreshed_at);
        assert_eq!(
            gateway.hash_params.location_asserts,
            gw_insert.location.map(|_| 1)
        );

        // Update sample gateways
        gateway_metadata_db::update_gateway(&pool, &gw_insert.asset, new_loc, now, 2).await?;
    }

    // now run the tracker again after updates
    tracker::execute(&pool, &pool).await?;

    // We should have TOTAL + sample_size gateways in the DB
    let total = count_gateways(&pool).await?;
    assert_eq!(TOTAL as i64 + sample_size as i64, total);

    for gw_insert in sample.clone() {
        let gateway = Gateway::get_by_address(&pool, &gw_insert.key)
            .await?
            .expect("gateway not found");

        assert_eq!(gateway.address, gw_insert.key.clone());
        assert_eq!(gateway.hash_params.gateway_type, GatewayType::WifiIndoor);
        assert_eq!(gateway.created_at, gw_insert.created_at);
        assert_eq!(gateway.last_changed_at, now);
        assert_eq!(gateway.hash_params.antenna, None);
        assert_eq!(gateway.hash_params.elevation, None);
        assert_eq!(gateway.hash_params.azimuth, None);
        assert_eq!(gateway.hash_params.location, Some(0));
        assert_eq!(gateway.location_changed_at, Some(now));
        assert_eq!(gateway.hash_params.location_asserts, Some(2));
    }

    Ok(())
}

async fn count_gateways(pool: &PgPool) -> anyhow::Result<i64> {
    let count = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM gateways;
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(count)
}

#[sqlx::test]
async fn test_gateway_tracker_owner_tracking(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now()
        .with_nanosecond(Utc::now().timestamp_subsec_micros() * 1000)
        .unwrap();

    // ensure tables exist
    gateway_metadata_db::create_tables(&pool).await;

    // Create a gateway with owner information
    let pubkey: helium_crypto::PublicKeyBinary = make_keypair().public_key().clone().into();
    let asset = "test_asset_001".to_string();
    let initial_owner = "owner1_address".to_string();
    let hex_val = 631_711_281_837_647_359_i64;

    let gateway = gateway_metadata_db::GatewayInsert {
        asset: asset.clone(),
        location: Some(hex_val),
        device_type: "\"wifiIndoor\"".to_string(),
        key: pubkey.clone(),
        created_at: now,
        refreshed_at: Some(now),
        deployment_info: None,
    };

    // Insert the gateway into mobile_hotspot_infos and key_to_assets
    gateway_metadata_db::insert_gateway_bulk(&pool, &[gateway], 1000).await?;

    // Insert the owner into asset_owners table
    gateway_metadata_db::insert_asset_owner(&pool, &asset, &initial_owner, now, now).await?;

    // Run the tracker execute function
    tracker::execute(&pool, &pool).await?;

    // Verify the gateway was created with the correct owner
    let retrieved_gateway = Gateway::get_by_address(&pool, &pubkey)
        .await?
        .expect("gateway not found");

    assert_eq!(retrieved_gateway.address, pubkey.clone());
    assert_eq!(
        retrieved_gateway.hash_params.gateway_type,
        GatewayType::WifiIndoor
    );
    assert_eq!(
        retrieved_gateway.hash_params.owner,
        Some(initial_owner.clone())
    );
    assert_eq!(retrieved_gateway.owner_changed_at, Some(now));

    // Count gateways before owner change
    let count_before = count_gateways(&pool).await?;
    assert_eq!(1, count_before);

    // Update the owner in asset_owners table
    let new_owner = "owner2_address".to_string();
    let update_time = now + chrono::Duration::hours(1);

    // Update the refreshed_at time in mobile_hotspot_infos to simulate a new update
    gateway_metadata_db::update_gateway(&pool, &asset, hex_val, update_time, 1).await?;

    gateway_metadata_db::update_asset_owner(&pool, &asset, &new_owner, update_time).await?;

    // Run tracker::execute again
    tracker::execute(&pool, &pool).await?;

    // Verify a new record was created after owner change
    let count_after = count_gateways(&pool).await?;
    assert_eq!(
        2, count_after,
        "A new record should be created when owner changes"
    );

    // Verify the owner and owner_changed_at were updated
    let updated_gateway = Gateway::get_by_address(&pool, &pubkey)
        .await?
        .expect("gateway not found");

    assert_eq!(updated_gateway.address, pubkey.clone());
    assert_eq!(updated_gateway.hash_params.owner, Some(new_owner.clone()));
    assert_eq!(updated_gateway.owner_changed_at, Some(update_time));
    assert_eq!(updated_gateway.last_changed_at, update_time);

    Ok(())
}
