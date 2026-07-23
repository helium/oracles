use crate::common::{
    self,
    gateway_db::{insert_history_version, GatewayTestExt},
};
use chrono::{TimeZone, Utc};
use futures::{pin_mut, StreamExt};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::db::{Gateway, GatewayType};
use sqlx::PgPool;

#[sqlx::test]
async fn get_by_address(pool: PgPool) -> anyhow::Result<()> {
    let addr = pk_binary();
    let now = Utc::now();

    gw(addr.clone(), GatewayType::WifiIndoor, now)
        .insert(&pool)
        .await?;

    let gateway = Gateway::get_by_address(&pool, &addr)
        .await?
        .expect("gateway should exist");

    assert_eq!(gateway.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(gateway.created_at, common::nanos_trunc(now));
    assert_eq!(gateway.last_changed_at, common::nanos_trunc(now));
    assert_eq!(gateway.location, Some(123));
    // antenna/elevation come from the deployment_info join
    assert_eq!(gateway.antenna, Some(1));
    assert_eq!(gateway.elevation, Some(2));
    Ok(())
}

#[sqlx::test]
async fn get_by_address_as_of_returns_version_at_time(pool: PgPool) -> anyhow::Result<()> {
    let addr = pk_binary();
    let t_old = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let t_new = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();

    // Two history versions with different locations.
    insert_history_version(
        &pool,
        &addr,
        GatewayType::WifiIndoor,
        t_old,
        Some(111),
        Some(10),
    )
    .await?;
    insert_history_version(
        &pool,
        &addr,
        GatewayType::WifiIndoor,
        t_new,
        Some(222),
        Some(20),
    )
    .await?;

    // Querying just after t_old returns the old version.
    let just_after_old = t_old + chrono::Duration::minutes(1);
    let gateway = Gateway::get_by_address_as_of(&pool, &addr, &just_after_old)
        .await?
        .expect("gateway should exist");
    assert_eq!(gateway.location, Some(111));
    assert_eq!(gateway.azimuth, Some(10));
    assert_eq!(gateway.created_at, common::nanos_trunc(t_old));

    // Querying later returns the newer version.
    let later = t_new + chrono::Duration::minutes(1);
    let gateway = Gateway::get_by_address_as_of(&pool, &addr, &later)
        .await?
        .expect("gateway should exist");
    assert_eq!(gateway.location, Some(222));
    assert_eq!(gateway.azimuth, Some(20));

    // Before any version -> none.
    let before = t_old - chrono::Duration::minutes(1);
    assert!(Gateway::get_by_address_as_of(&pool, &addr, &before)
        .await?
        .is_none());

    Ok(())
}

#[sqlx::test]
async fn get_by_addresses(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let a1 = pk_binary();
    let a2 = pk_binary();
    let a3 = pk_binary();

    gw(a1.clone(), GatewayType::WifiIndoor, now)
        .insert(&pool)
        .await?;
    gw(a2.clone(), GatewayType::WifiOutdoor, now)
        .insert(&pool)
        .await?;
    gw(a3.clone(), GatewayType::WifiDataOnly, now)
        .insert(&pool)
        .await?;

    let rows = Gateway::get_by_addresses(&pool, vec![a1.clone(), a2.clone(), a3.clone()]).await?;
    assert_eq!(rows.len(), 3);
    Ok(())
}

#[sqlx::test]
async fn stream_by_addresses_filters_by_min_last_changed_at(pool: PgPool) -> anyhow::Result<()> {
    let a1 = pk_binary();
    let a2 = pk_binary();
    let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

    gw(a1.clone(), GatewayType::WifiIndoor, t1)
        .insert(&pool)
        .await?;
    gw(a2.clone(), GatewayType::WifiDataOnly, t2)
        .insert(&pool)
        .await?;

    // min_last_changed_at = t2 => only a2
    let s = Gateway::stream_by_addresses(&pool, vec![a1.clone(), a2.clone()], t2);
    pin_mut!(s);
    let first = s.next().await.expect("one row");
    assert_eq!(first.address, a2);
    assert!(s.next().await.is_none());

    Ok(())
}

#[sqlx::test]
async fn stream_by_types_filters_by_min_date(pool: PgPool) -> anyhow::Result<()> {
    let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

    gw(pk_binary(), GatewayType::WifiIndoor, t0)
        .insert(&pool)
        .await?;
    gw(pk_binary(), GatewayType::WifiOutdoor, t1)
        .insert(&pool)
        .await?;
    let key = pk_binary();
    gw(key.clone(), GatewayType::WifiIndoor, t2)
        .insert(&pool)
        .await?;

    let s = Gateway::stream_by_types(&pool, vec![GatewayType::WifiIndoor], t2, None);
    pin_mut!(s);
    let first = s.next().await.expect("row expected");
    assert_eq!(first.gateway_type, GatewayType::WifiIndoor);
    assert_eq!(first.address, key);
    assert!(s.next().await.is_none());

    Ok(())
}

#[sqlx::test]
async fn stream_by_types_optional_location_changed_filter(pool: PgPool) -> anyhow::Result<()> {
    let t0 = Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2025, 2, 3, 0, 0, 0).unwrap();

    let a_addr = pk_binary();
    let mut a = gw(a_addr.clone(), GatewayType::WifiIndoor, t0);
    a.location_changed_at = Some(t0);
    a.insert(&pool).await?;

    let b_addr = pk_binary();
    let mut b = gw(b_addr.clone(), GatewayType::WifiIndoor, t1);
    b.location_changed_at = Some(t1);
    b.insert(&pool).await?;

    // min_date = t0; min_location_changed_at = t1 => only b
    let s = Gateway::stream_by_types(&pool, vec![GatewayType::WifiIndoor], t0, Some(t1));
    pin_mut!(s);
    let first = s.next().await.expect("row expected");
    assert_eq!(first.address, b_addr);
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
        last_changed_at: t,
        antenna: Some(1),
        elevation: Some(2),
        azimuth: Some(180),
        location: Some(123),
        location_changed_at: None,
        location_asserts: Some(5),
        owner: None,
        owner_changed_at: Some(t),
    }
}
