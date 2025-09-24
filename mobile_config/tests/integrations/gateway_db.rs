use chrono::{TimeZone, Utc};
use futures::{pin_mut, StreamExt};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::db::{Gateway, GatewayType};
use sqlx::PgPool;

use crate::common;

#[sqlx::test]
async fn gateway_insert_and_get_by_address(pool: PgPool) -> anyhow::Result<()> {
    let addr = pk_binary();
    let now = Utc::now();

    let gateway = gw(addr.clone(), GatewayType::WifiIndoor, now);
    gateway.insert(&pool).await?;

    let gateway = Gateway::get_by_address(&pool, &addr)
        .await?
        .expect("gateway should exist");

    assert_eq!(gateway.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(gateway.created_at, common::nanos_trunc(now));
    assert_eq!(gateway.updated_at, common::nanos_trunc(now));
    assert_eq!(gateway.refreshed_at, common::nanos_trunc(now));
    assert_eq!(gateway.last_changed_at, common::nanos_trunc(now)); // first insert: equals refreshed_at
    assert_eq!(gateway.location, Some(123));
    assert_eq!(gateway.hash, "h0");
    Ok(())
}

#[sqlx::test]
async fn gateway_upsert_last_changed_at_on_location_or_hash_change(
    pool: PgPool,
) -> anyhow::Result<()> {
    let addr = pk_binary();
    let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();
    let t3 = Utc.with_ymd_and_hms(2025, 1, 4, 0, 0, 0).unwrap();

    // insert baseline
    gw(addr.clone(), GatewayType::WifiOutdoor, t0)
        .insert(&pool)
        .await?;

    // upsert with no change (only timestamps move)
    let mut same = gw(addr.clone(), GatewayType::WifiOutdoor, t0);
    same.updated_at = t1;
    same.refreshed_at = t1;
    same.last_changed_at = t1; // should be ignored by SQL if no change
    same.insert(&pool).await?;

    let after_same = Gateway::get_by_address(&pool, &addr).await?.unwrap();
    assert_eq!(after_same.refreshed_at, t1);
    assert_eq!(after_same.last_changed_at, t0); // unchanged

    // upsert with location change -> last_changed_at bumps to refreshed_at (t2)
    let mut loc = after_same.clone();
    loc.updated_at = t2;
    loc.refreshed_at = t2;
    loc.location = Some(456);
    loc.insert(&pool).await?;

    let after_loc = Gateway::get_by_address(&pool, &addr).await?.unwrap();
    assert_eq!(after_loc.location, Some(456));
    assert_eq!(after_loc.last_changed_at, t2);

    // upsert with hash change (location same) -> last_changed_at bumps again
    let mut h = after_loc.clone();
    h.updated_at = t3;
    h.refreshed_at = t3;
    h.hash = "h1".into();
    h.insert(&pool).await?;

    let after_hash = Gateway::get_by_address(&pool, &addr).await?.unwrap();
    assert_eq!(after_hash.hash, "h1");
    assert_eq!(after_hash.last_changed_at, t3);

    Ok(())
}

#[sqlx::test]
async fn gateway_bulk_insert_and_get(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();

    let a1 = pk_binary();
    let a2 = pk_binary();
    let a3 = pk_binary();

    let g1 = gw(a1.clone(), GatewayType::WifiIndoor, now);
    let g2 = gw(a2.clone(), GatewayType::WifiOutdoor, now);
    let g3 = gw(a3.clone(), GatewayType::WifiDataOnly, now);

    let affected = Gateway::insert_bulk(&pool, &[g1, g2, g3]).await?;
    assert_eq!(affected, 3, "should insert 3 rows");

    for addr in [&a1, &a2, &a3] {
        let got = Gateway::get_by_address(&pool, addr)
            .await?
            .expect("row should exist");
        assert_eq!(got.created_at, common::nanos_trunc(now));
        assert_eq!(got.updated_at, common::nanos_trunc(now));
        assert_eq!(got.refreshed_at, common::nanos_trunc(now));
        assert_eq!(got.last_changed_at, common::nanos_trunc(now));
        assert_eq!(got.location, Some(123));
        assert_eq!(got.hash, "h0");
    }

    Ok(())
}

#[sqlx::test]
async fn gateway_bulk_upsert_updates_and_change(pool: PgPool) -> anyhow::Result<()> {
    // seed two rows at t0
    let t0 = Utc::now();
    let a1 = pk_binary();
    let a2 = pk_binary();

    let mut g1 = gw(a1.clone(), GatewayType::WifiIndoor, t0);
    let mut g2 = gw(a2.clone(), GatewayType::WifiOutdoor, t0);
    let _ = Gateway::insert_bulk(&pool, &[g1.clone(), g2.clone()]).await?;

    // upsert at t1: change only timestamps for g1 (no loc/hash change)
    // and change location for g2 (should bump last_changed_at)
    let t1 = Utc::now();

    g1.updated_at = t1;
    g1.refreshed_at = t1;
    // leave g1.location / g1.hash the same

    g2.updated_at = t1;
    g2.refreshed_at = t1;
    g2.location = Some(456); // change => last_changed_at should bump to t1
                             // g2.hash unchanged

    let affected = Gateway::insert_bulk(&pool, &[g1.clone(), g2.clone()]).await?;
    // 2 rows should be affected (both upserted)
    assert_eq!(affected, 2);

    // verify g1: timestamps updated, last_changed_at unchanged (no relevant change)
    let got1 = Gateway::get_by_address(&pool, &a1)
        .await?
        .expect("row should exist");
    assert_eq!(got1.updated_at, common::nanos_trunc(t1));
    assert_eq!(got1.refreshed_at, common::nanos_trunc(t1));
    assert_eq!(
        got1.last_changed_at,
        common::nanos_trunc(t0),
        "no loc/hash change ⇒ last_changed_at stays t0"
    );
    assert_eq!(got1.location, Some(123));
    assert_eq!(got1.hash, "h0");

    // verify g2: timestamps updated, last_changed_at bumped due to location change
    let got2 = Gateway::get_by_address(&pool, &a2)
        .await?
        .expect("row should exist");
    assert_eq!(got2.updated_at, common::nanos_trunc(t1));
    assert_eq!(got2.refreshed_at, common::nanos_trunc(t1));
    assert_eq!(
        got2.last_changed_at,
        common::nanos_trunc(t1),
        "location changed ⇒ last_changed_at = refreshed_at"
    );
    assert_eq!(got2.location, Some(456));
    assert_eq!(got2.hash, "h0");

    // second upsert at t2: change hash only for g1, ensure bump
    let t2 = Utc::now();

    g1.updated_at = t2;
    g1.refreshed_at = t2;
    g1.hash = "h1".into(); // change ⇒ bump last_changed_at

    let affected2 = Gateway::insert_bulk(&pool, &[g1.clone()]).await?;
    assert_eq!(affected2, 1);

    let got1b = Gateway::get_by_address(&pool, &a1)
        .await?
        .expect("row should exist");
    assert_eq!(got1b.updated_at, common::nanos_trunc(t2));
    assert_eq!(got1b.refreshed_at, common::nanos_trunc(t2));
    assert_eq!(
        got1b.last_changed_at,
        common::nanos_trunc(t2),
        "hash changed ⇒ last_changed_at = refreshed_at"
    );
    assert_eq!(got1b.hash, "h1");

    Ok(())
}

#[sqlx::test]
async fn stream_by_addresses_filters_by_min_updated(pool: PgPool) -> anyhow::Result<()> {
    let a1 = pk_binary();
    let a2 = pk_binary();
    let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

    let mut g1 = gw(a1.clone(), GatewayType::WifiIndoor, t0);
    g1.hash = "x".into();
    g1.insert(&pool).await?;

    let mut g2 = gw(a2.clone(), GatewayType::WifiDataOnly, t1);
    g2.hash = "y".into();
    g2.insert(&pool).await?;

    // min_updated_at = t2 => expect empty
    let s = Gateway::stream_by_addresses(&pool, vec![a1.clone(), a2.clone()], t2);
    pin_mut!(s);
    assert!(s.next().await.is_none());

    // bump g1.updated_at to t2
    let mut g1b = g1.clone();
    g1b.updated_at = t2;
    g1b.refreshed_at = t0;
    g1b.insert(&pool).await?;

    // now we should see g1 only
    let s = Gateway::stream_by_addresses(&pool, vec![a1.clone(), a2.clone()], t2);
    pin_mut!(s);
    let first = s.next().await.expect("one row");
    assert_eq!(first.address.as_ref(), a1.as_ref());
    assert!(s.next().await.is_none());

    Ok(())
}

#[sqlx::test]
async fn stream_by_types_filters_by_min_date(pool: PgPool) -> anyhow::Result<()> {
    let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

    // Insert: two WifiIndoor (t0, t2) and one WifiOutdoor (t1)
    gw(pk_binary(), GatewayType::WifiIndoor, t0)
        .insert(&pool)
        .await?;
    gw(pk_binary(), GatewayType::WifiOutdoor, t1)
        .insert(&pool)
        .await?;
    gw(pk_binary(), GatewayType::WifiIndoor, t2)
        .insert(&pool)
        .await?;

    // min_date = t2, types = WifiIndoor; expect only the t2 indoor
    let s = Gateway::stream_by_types(&pool, vec![GatewayType::WifiIndoor], t2, None);
    pin_mut!(s);
    let first = s.next().await.expect("row expected");
    assert_eq!(first.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(first.created_at, t2);
    assert!(s.next().await.is_none());

    Ok(())
}

#[sqlx::test]
async fn stream_by_types_optional_location_changed_filter(pool: PgPool) -> anyhow::Result<()> {
    let t0 = Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 2, 3, 0, 0, 0).unwrap();

    // Indoor @ t0 and Indoor @ t1; set location_changed_at accordingly
    let a_addr = pk_binary();
    let mut a = gw(a_addr.clone(), GatewayType::WifiIndoor, t0);
    a.location_changed_at = Some(t0);
    a.insert(&pool).await?;

    let b_addr = pk_binary();
    let mut b = gw(b_addr.clone(), GatewayType::WifiIndoor, t1);
    b.location_changed_at = Some(t1);
    b.insert(&pool).await?;

    // min_date = t0 (so date doesn't exclude); set min_location_changed_at = t1
    let s = Gateway::stream_by_types(&pool, vec![GatewayType::WifiIndoor], t0, Some(t1));
    pin_mut!(s);

    // Expect only the one with location_changed_at >= t1 (i.e., `b`)
    let first = s.next().await.expect("row expected");
    assert_eq!(first.address.as_ref(), b_addr.as_ref());
    assert!(s.next().await.is_none());

    Ok(())
}

fn pk_binary() -> PublicKeyBinary {
    common::make_keypair().public_key().clone().into()
}

fn gw(address: PublicKeyBinary, gateway_type: GatewayType, t: chrono::DateTime<Utc>) -> Gateway {
    Gateway {
        address,
        gateway_type,
        created_at: t,
        updated_at: t,
        refreshed_at: t,
        last_changed_at: t, // initialize; SQL will manage on upsert
        hash: "h0".to_string(),
        antenna: Some(1),
        elevation: Some(2),
        azimuth: Some(180),
        location: Some(123),
        location_changed_at: None,
        location_asserts: Some(5),
    }
}
