use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use iot_config::gateway::db::Gateway;
use sqlx::PgPool;

use crate::common;

#[sqlx::test]
async fn gateway_bulk_insert_and_get(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();

    let a1 = pk_binary();
    let a2 = pk_binary();
    let a3 = pk_binary();

    let g1 = gw(a1.clone(), now);
    let g2 = gw(a2.clone(), now);
    let g3 = gw(a3.clone(), now);

    let affected = Gateway::insert_bulk(&pool, &[g1, g2, g3]).await?;
    assert_eq!(affected, 3, "should insert 3 rows");

    for addr in [&a1, &a2, &a3] {
        let got = Gateway::get_by_address(&pool, addr)
            .await?
            .expect("row should exist");
        assert_eq!(got.created_at, common::nanos_trunc(now));
        assert_eq!(got.elevation, Some(1));
        assert_eq!(got.gain, Some(2));
        assert_eq!(got.hash, "h0");
        assert_eq!(got.is_active, Some(true));
        assert_eq!(got.is_full_hotspot, Some(true));
        assert_eq!(got.last_changed_at, common::nanos_trunc(now));
        assert_eq!(got.location, Some(3));
        assert_eq!(got.location_asserts, Some(4));
        assert!(got.location_changed_at.is_some());
        assert_eq!(got.location_changed_at.unwrap(), common::nanos_trunc(now));
        assert!(got.refreshed_at.is_some());
        assert_eq!(got.refreshed_at.unwrap(), common::nanos_trunc(now));
        assert_eq!(got.updated_at, common::nanos_trunc(now));
    }

    Ok(())
}

fn pk_binary() -> PublicKeyBinary {
    common::make_keypair().public_key().clone().into()
}

fn gw(address: PublicKeyBinary, ts: chrono::DateTime<Utc>) -> Gateway {
    Gateway {
        address,
        created_at: ts,
        elevation: Some(1),
        gain: Some(2),
        hash: "h0".to_string(),
        is_active: Some(true),
        is_full_hotspot: Some(true),
        last_changed_at: ts,
        location: Some(3),
        location_asserts: Some(4),
        location_changed_at: Some(ts),
        refreshed_at: Some(ts),
        updated_at: ts,
    }
}
