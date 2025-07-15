use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use mobile_config::mobile_radio_tracker::{get_tracked_radios, track_changes};
use sqlx::PgPool;

pub mod common;
use common::*;

#[sqlx::test]
async fn mt_handle_entity_duplicates(pool: PgPool) {
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
        Some(asset1_hex_idx),
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
        Some(asset1_hex_idx),
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
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now,
        Some(now),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    let b58 = bs58::decode(pubkey_binary.to_string()).into_vec().unwrap();
    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();
    assert_eq!(tracked_radios.len(), 1);
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(
        tracked_radio.last_changed_at.timestamp_millis(),
        now.timestamp_millis()
    );
}

#[sqlx::test]
async fn mt_update_radio_location_none(pool: PgPool) {
    // 1. Add a new radio without location
    // 2. Update radio, set location
    let asset2_pubkey = make_keypair().public_key().clone();
    create_db_tables(&pool).await;
    let now = Utc::now();
    let now_minus_hour = now - chrono::Duration::hours(1);
    let pubkey2_binary = PublicKeyBinary::from(asset2_pubkey.clone());

    add_db_record(
        &pool,
        "asset2",
        None,
        "\"wifiIndoor\"",
        asset2_pubkey.clone().into(),
        now_minus_hour,
        Some(now_minus_hour),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();

    // Check radio with location none
    let b58 = bs58::decode(pubkey2_binary.to_string()).into_vec().unwrap();
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert!(tracked_radio.asserted_location.is_none());
    assert!(tracked_radio.asserted_location_changed_at.is_none());

    // Update radio, set new location
    sqlx::query(
        r#"
           UPDATE 
"mobile_hotspot_infos" SET location = $1, refreshed_at = $2, num_location_asserts = 1 WHERE asset = 'asset2'
    "#,
    )
    .bind(12)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();

    let b58 = bs58::decode(pubkey2_binary.to_string()).into_vec().unwrap();
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(tracked_radio.asserted_location, Some(12));
    assert_eq!(
        tracked_radio
            .asserted_location_changed_at
            .unwrap()
            .timestamp_millis(),
        now.timestamp_millis()
    );
}

#[sqlx::test]
async fn mt_update_radio_location_exist(pool: PgPool) {
    // 1. Add a new radio with location
    // 2. Update radio, set a new location
    let asset1_pubkey = make_keypair().public_key().clone();
    let asset1_hex_idx = 631711281837647359_i64;
    create_db_tables(&pool).await;
    let now = Utc::now();
    let now_minus_hour = now - chrono::Duration::hours(1);
    let pubkey_binary = PublicKeyBinary::from(asset1_pubkey.clone());

    add_db_record(
        &pool,
        "asset1",
        Some(asset1_hex_idx),
        "\"wifiIndoor\"",
        asset1_pubkey.clone().into(),
        now_minus_hour,
        Some(now_minus_hour),
        Some(r#"{"wifiInfoV0": {"antenna": 18, "azimuth": 160, "elevation": 5, "electricalDownTilt": 1, "mechanicalDownTilt": 2}}"#)
    )
    .await;

    let b58 = bs58::decode(pubkey_binary.to_string()).into_vec().unwrap();
    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();
    assert_eq!(tracked_radios.len(), 1);
    // Check radio with location is not None
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(
        tracked_radio.last_changed_at.timestamp_millis(),
        now_minus_hour.timestamp_millis()
    );
    assert_eq!(
        tracked_radio
            .asserted_location_changed_at
            .unwrap()
            .timestamp_millis(),
        now_minus_hour.timestamp_millis()
    );

    // Update radio with location none
    sqlx::query(
        r#"
           UPDATE 
"mobile_hotspot_infos" SET location = $1, refreshed_at = $2, num_location_asserts = 2 WHERE asset = 'asset1'
    "#,
    )
    .bind(12)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(tracked_radio.asserted_location, Some(12));
    assert_eq!(
        tracked_radio
            .asserted_location_changed_at
            .unwrap()
            .timestamp_millis(),
        now.timestamp_millis()
    );
}
