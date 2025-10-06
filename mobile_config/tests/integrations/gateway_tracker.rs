use crate::common::{gateway_metadata_db, make_keypair};
use chrono::{Timelike, Utc};
use custom_tracing::Settings;
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    tracker,
};
use rand::{seq::SliceRandom, thread_rng};
use sqlx::PgPool;

#[sqlx::test]
async fn execute_test(pool: PgPool) -> anyhow::Result<()> {
    const TOTAL: usize = 10_000;

    custom_tracing::init("mobile_config=debug,info".to_string(), Settings::default()).await?;

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
        gateway_metadata_db::update_gateway(&pool, &gw_insert.asset, new_loc, now).await?;
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
    let count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM gateways;
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(count.0)
}
