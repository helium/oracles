use std::sync::Arc;

use chrono::{Days, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::GatewayClient;
use mobile_config::mobile_radio_tracker::{
    get_tracked_radios, MobileRadioTracker, TrackedRadiosMap,
};
use sqlx::PgPool;
use tokio::sync::RwLock;

pub mod common;
use common::*;

#[sqlx::test]
async fn mobile_tracker_integration_test(pool: PgPool) {
    let admin_key = make_keypair();
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    let created_at = Utc::now() - Duration::hours(5);
    let refreshed_at = Utc::now() - Duration::hours(3);

    create_db_tables(&pool).await;
    add_db_record(
        &pool,
        "asset1",
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        created_at,
        Some(refreshed_at),
        None,
    )
    .await;
    let (addr, _handle, mobile_tracker) =
        spawn_gateway_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = GatewayClient::connect(addr).await.unwrap();
    let req = make_signed_info_request(&asset1_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();

    let gw_info = resp.info.unwrap();
    assert_eq!(gw_info.updated_at, refreshed_at.timestamp() as u64);

    sqlx::query("UPDATE mobile_hotspot_infos SET refreshed_at = $1")
        .bind(refreshed_at.checked_add_days(Days::new(1)).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    mobile_tracker.track_changes().await.unwrap();
    let req = make_signed_info_request(&asset1_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();
    let gw_info = resp.info.unwrap();
    assert_eq!(gw_info.updated_at, refreshed_at.timestamp() as u64);

    let new_updated_at = refreshed_at.checked_add_days(Days::new(2)).unwrap();
    sqlx::query("UPDATE mobile_hotspot_infos SET refreshed_at = $1, location = $2")
        .bind(new_updated_at)
        .bind(0x8c446ca9aae35ff_i64)
        .execute(&pool)
        .await
        .unwrap();
    mobile_tracker.track_changes().await.unwrap();
    let req = make_signed_info_request(&asset1_pubkey, &admin_key);
    let resp = client.info_v2(req).await.unwrap().into_inner();
    let gw_info = resp.info.unwrap();
    assert_eq!(gw_info.updated_at, new_updated_at.timestamp() as u64);
}

#[sqlx::test]
async fn mobile_tracker_handle_entity_duplicates(pool: PgPool) {
    // In case of duplications mobile tracker must use newer (refreshed_at)
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    create_db_tables(&pool).await;
    let now = Utc::now();
    let now_minus_hour = now - chrono::Duration::hours(1);
    let pubkey_binary = PublicKeyBinary::from(asset1_pubkey.clone());

    add_db_record(
        &pool,
        "asset1",
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now_minus_hour,
        Some(now_minus_hour),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset1",
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        None,
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    add_db_record(
        &pool,
        "asset1",
        asset1_hex_idx,
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    let b58 = bs58::decode(pubkey_binary.to_string()).into_vec().unwrap();

    let tracked_radios_cache: Arc<RwLock<TrackedRadiosMap>> =
        Arc::new(RwLock::new(TrackedRadiosMap::new()));

    let mobile_tracker = MobileRadioTracker::new(
        pool.clone(),
        pool.clone(),
        humantime::parse_duration("1 hour").unwrap(),
        Arc::clone(&tracked_radios_cache),
    );
    mobile_tracker.track_changes().await.unwrap();

    let tracked_radios = get_tracked_radios(&pool).await.unwrap();
    assert_eq!(tracked_radios.len(), 1);
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(
        tracked_radio.last_changed_at.timestamp_millis(),
        now.timestamp_millis()
    );
}
