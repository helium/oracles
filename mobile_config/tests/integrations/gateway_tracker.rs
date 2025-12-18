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
        assert_eq!(gateway.gateway_type, GatewayType::WifiIndoor);
        assert_eq!(gateway.created_at, gw_insert.created_at);
        assert_eq!(Some(gateway.refreshed_at), gw_insert.refreshed_at);
        assert_eq!(Some(gateway.last_changed_at), gw_insert.refreshed_at);
        assert_eq!(gateway.antenna, None);
        assert_eq!(gateway.elevation, None);
        assert_eq!(gateway.azimuth, None);
        assert_eq!(gateway.location, gw_insert.location.map(|v| v as u64));
        assert_eq!(gateway.location_changed_at, gw_insert.refreshed_at); // matches logic in tracker
        assert_eq!(gateway.location_asserts, gw_insert.location.map(|_| 1));

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
        assert_eq!(gateway.gateway_type, GatewayType::WifiIndoor);
        assert_eq!(gateway.created_at, gw_insert.created_at);
        assert_eq!(gateway.refreshed_at, now);
        assert_eq!(gateway.last_changed_at, now);
        assert_eq!(gateway.antenna, None);
        assert_eq!(gateway.elevation, None);
        assert_eq!(gateway.azimuth, None);
        assert_eq!(gateway.location, Some(0));
        assert_eq!(gateway.location_changed_at, Some(now));
        assert_eq!(gateway.location_asserts, Some(2));
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
    assert_eq!(retrieved_gateway.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(retrieved_gateway.owner, Some(initial_owner.clone()));
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
    assert_eq!(updated_gateway.owner, Some(new_owner.clone()));
    assert_eq!(updated_gateway.owner_changed_at, Some(update_time));
    // last_changed_at should also be updated since owner changed (In next owner implementing stage).
    // But currently, it stays the same. After full implementation change `now` to `update_time`
    assert_eq!(updated_gateway.last_changed_at, now);

    Ok(())
}

#[sqlx::test]
async fn test_backfill_gateway_owners(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now()
        .with_nanosecond(Utc::now().timestamp_subsec_micros() * 1000)
        .unwrap();

    // ensure tables exist
    gateway_metadata_db::create_tables(&pool).await;

    // Create gateways without owners initially
    let gateway1_pubkey: helium_crypto::PublicKeyBinary =
        make_keypair().public_key().clone().into();
    let gateway2_pubkey: helium_crypto::PublicKeyBinary =
        make_keypair().public_key().clone().into();

    let asset1 = "test_asset_backfill_001".to_string();
    let asset2 = "test_asset_backfill_002".to_string();
    let hex_val = 631_711_281_837_647_359_i64;

    let gateways = vec![
        gateway_metadata_db::GatewayInsert {
            asset: asset1.clone(),
            location: Some(hex_val),
            device_type: "\"wifiIndoor\"".to_string(),
            key: gateway1_pubkey.clone(),
            created_at: now,
            refreshed_at: Some(now),
            deployment_info: None,
        },
        gateway_metadata_db::GatewayInsert {
            asset: asset2.clone(),
            location: Some(hex_val + 1),
            device_type: "\"wifiOutdoor\"".to_string(),
            key: gateway2_pubkey.clone(),
            created_at: now,
            refreshed_at: Some(now),
            deployment_info: None,
        },
    ];

    // Directly insert gateways into the gateways table WITHOUT owners
    // This simulates the state before the owner tracking feature was added
    let gw1 = Gateway {
        address: gateway1_pubkey.clone(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: now,
        inserted_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "test_hash_1".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some(hex_val as u64),
        location_changed_at: Some(now),
        location_asserts: Some(1),
        owner: None,
        owner_changed_at: None,
    };

    let gw2 = Gateway {
        address: gateway2_pubkey.clone(),
        gateway_type: GatewayType::WifiOutdoor,
        created_at: now,
        inserted_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "test_hash_2".to_string(),
        antenna: None,
        elevation: None,
        azimuth: None,
        location: Some((hex_val + 1) as u64),
        location_changed_at: Some(now),
        location_asserts: Some(1),
        owner: None,
        owner_changed_at: None,
    };

    Gateway::insert_bulk(&pool, &[gw1.clone(), gw2.clone()]).await?;

    // Verify gateways were created with NULL owners
    let gw1_before = Gateway::get_by_address(&pool, &gateway1_pubkey)
        .await?
        .expect("gateway 1 not found");
    let gw2_before = Gateway::get_by_address(&pool, &gateway2_pubkey)
        .await?
        .expect("gateway 2 not found");

    assert_eq!(gw1_before.owner, None, "Gateway 1 should have NULL owner");
    assert_eq!(
        gw1_before.owner_changed_at, None,
        "Gateway 1 should have NULL owner_changed_at"
    );
    assert_eq!(gw2_before.owner, None, "Gateway 2 should have NULL owner");
    assert_eq!(
        gw2_before.owner_changed_at, None,
        "Gateway 2 should have NULL owner_changed_at"
    );

    // Insert gateways into metadata tables with owners
    gateway_metadata_db::insert_gateway_bulk(&pool, &gateways, 1000).await?;

    // Now add owners to the metadata database
    let owner1 = "owner1_backfill_address".to_string();
    let owner2 = "owner2_backfill_address".to_string();

    gateway_metadata_db::insert_asset_owner(&pool, &asset1, &owner1, now, now).await?;
    gateway_metadata_db::insert_asset_owner(&pool, &asset2, &owner2, now, now).await?;

    // Count gateway records before backfill
    let count_before = count_gateways(&pool).await?;
    assert_eq!(
        2, count_before,
        "Should have 2 gateway records before backfill"
    );

    // Run backfill_gateway_owners
    tracker::backfill_gateway_owners(&pool, &pool).await?;

    // Verify no new records were added (backfill should UPDATE, not INSERT)
    let count_after = count_gateways(&pool).await?;
    assert_eq!(
        count_before, count_after,
        "Backfill should not create new records, only update existing ones"
    );

    // Verify owners were updated
    let gw1_after = Gateway::get_by_address(&pool, &gateway1_pubkey)
        .await?
        .expect("gateway 1 not found after backfill");
    let gw2_after = Gateway::get_by_address(&pool, &gateway2_pubkey)
        .await?
        .expect("gateway 2 not found after backfill");

    assert_eq!(
        gw1_after.owner,
        Some(owner1.clone()),
        "Gateway 1 owner should be updated"
    );
    assert_eq!(
        gw1_after.owner_changed_at,
        Some(gw1_before.last_changed_at),
        "Gateway 1 owner_changed_at should be set to last_changed_at"
    );

    assert_eq!(
        gw2_after.owner,
        Some(owner2.clone()),
        "Gateway 2 owner should be updated"
    );
    assert_eq!(
        gw2_after.owner_changed_at,
        Some(gw2_before.last_changed_at),
        "Gateway 2 owner_changed_at should be set to last_changed_at"
    );

    // Verify that other fields remain unchanged
    assert_eq!(gw1_after.address, gw1_before.address);
    assert_eq!(gw1_after.gateway_type, gw1_before.gateway_type);
    assert_eq!(gw1_after.last_changed_at, gw1_before.last_changed_at);
    assert_eq!(gw1_after.location, gw1_before.location);
    assert_eq!(
        gw1_after.hash, gw1_before.hash,
        "Hash should not change during backfill"
    );

    assert_eq!(gw2_after.address, gw2_before.address);
    assert_eq!(gw2_after.gateway_type, gw2_before.gateway_type);
    assert_eq!(gw2_after.last_changed_at, gw2_before.last_changed_at);
    assert_eq!(gw2_after.location, gw2_before.location);
    assert_eq!(
        gw2_after.hash, gw2_before.hash,
        "Hash should not change during backfill"
    );

    Ok(())
}
