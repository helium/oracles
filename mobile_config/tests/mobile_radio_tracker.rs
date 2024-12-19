use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use mobile_config::mobile_radio_tracker::{get_tracked_radios, track_changes};
use sqlx::PgPool;

pub mod common;
use common::*;

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
    track_changes(&pool, &pool).await.unwrap();
    let tracked_radios = get_tracked_radios(&pool).await.unwrap();
    assert_eq!(tracked_radios.len(), 1);
    let tracked_radio = tracked_radios.get::<Vec<u8>>(&b58).unwrap();
    assert_eq!(
        tracked_radio.last_changed_at.timestamp_millis(),
        now.timestamp_millis()
    );
}
