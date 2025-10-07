use crate::common::{self, gateway_db::PreHistoricalGateway, partial_migrator::PartialMigrator};
use chrono::{Duration, Utc};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::db::{Gateway, GatewayType};
use sqlx::PgPool;

#[sqlx::test(migrations = false)]
async fn gateways_historical(pool: PgPool) -> anyhow::Result<()> {
    let partial_migrator = PartialMigrator::new(pool.clone(), vec![20251003000000], None).await?;

    partial_migrator.run_partial().await?;

    let address = pk_binary();
    let now = Utc::now();
    let one_min_ago = now - Duration::minutes(1);

    let pre_gw = PreHistoricalGateway {
        address: address.clone(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: one_min_ago,
        updated_at: now,
        refreshed_at: now,
        last_changed_at: now,
        hash: "h0".to_string(),
        antenna: Some(1),
        elevation: Some(2),
        azimuth: Some(3),
        location: Some(123),
        location_changed_at: Some(now),
        location_asserts: Some(1),
    };

    pre_gw.insert(&pool).await?;

    partial_migrator.run_skipped().await?;

    let gw = Gateway::get_by_address(&pool, &address)
        .await?
        .expect("should find gateway");

    println!("pre_gw: {:?}", pre_gw);
    println!("gw: {:?}", gw);

    assert!(pre_gw.address == gw.address);
    assert!(pre_gw.gateway_type == gw.gateway_type);
    assert!(pre_gw.created_at == common::nanos_trunc(gw.created_at));
    // The real change is updated_at renamed to inserted_at AND inserted_at = created_at;
    assert!(pre_gw.created_at == common::nanos_trunc(gw.inserted_at));
    assert!(pre_gw.refreshed_at == common::nanos_trunc(gw.refreshed_at));
    assert!(pre_gw.last_changed_at == common::nanos_trunc(gw.last_changed_at));
    assert!(pre_gw.hash == gw.hash);
    assert!(pre_gw.antenna == gw.antenna);
    assert!(pre_gw.elevation == gw.elevation);
    assert!(pre_gw.azimuth == gw.azimuth);
    assert!(pre_gw.location == gw.location);
    assert!(pre_gw.location_changed_at == gw.location_changed_at);
    assert!(pre_gw.location_asserts == gw.location_asserts);

    Ok(())
}

fn pk_binary() -> PublicKeyBinary {
    common::make_keypair().public_key().clone().into()
}
