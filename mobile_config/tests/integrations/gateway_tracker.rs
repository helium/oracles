use crate::common::{
    gateway_metadata_db::{self, GatewayInsert},
    make_keypair,
};
use chrono::{DateTime, TimeZone, Utc};
use helium_crypto::PublicKeyBinary;
use mobile_config::gateway::{
    db::{Gateway, GatewayType},
    tracker,
};
use sqlx::PgPool;

/// Seed a row in the local `gateways` table with the given antenna/elevation.
async fn seed_gateway(
    pool: &PgPool,
    pk: &PublicKeyBinary,
    antenna: Option<u32>,
    elevation: Option<u32>,
) -> anyhow::Result<Gateway> {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let gw = Gateway {
        address: pk.clone(),
        gateway_type: GatewayType::WifiIndoor,
        created_at: now,
        inserted_at: now,
        last_changed_at: now,
        hash: String::new(),
        antenna,
        elevation,
        azimuth: Some(33),
        location: Some(0x8528347ffffffff),
        location_changed_at: Some(now),
        location_asserts: Some(1),
        owner: None,
        owner_changed_at: None,
    };
    gw.insert(pool).await?;
    Ok(gw)
}

/// Seed `mobile_hotspot_infos` / `key_to_assets` with a deployment_info
/// JSON that produces the given antenna/elevation when fetched.
async fn seed_metadata_deployment(
    pool: &PgPool,
    pk: &PublicKeyBinary,
    deployment_info: Option<String>,
) -> anyhow::Result<()> {
    let now: DateTime<Utc> = Utc::now();
    gateway_metadata_db::insert_gateway_bulk(
        pool,
        &[GatewayInsert {
            asset: format!("asset-{pk}"),
            location: Some(1),
            device_type: "\"wifiIndoor\"".into(),
            key: pk.clone(),
            created_at: now,
            refreshed_at: Some(now),
            deployment_info,
        }],
        100,
    )
    .await?;
    Ok(())
}

fn wifi_deployment_info(antenna: u32, elevation: u32) -> String {
    serde_json::json!({
        "wifiInfoV0": {
            "antenna": antenna,
            "elevation": elevation,
            "azimuth": 0,
            "mechanicalDownTilt": 0,
            "electricalDownTilt": 0
        }
    })
    .to_string()
}

async fn row_count(pool: &PgPool, pk: &PublicKeyBinary) -> anyhow::Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM gateways WHERE address = $1")
            .bind(pk.as_ref())
            .fetch_one(pool)
            .await?,
    )
}

#[sqlx::test]
async fn fixes_null_antenna_elevation_when_metadata_db_catches_up(
    pool: PgPool,
) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, None, None).await?;
    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(11, 22))).await?;

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(11));
    assert_eq!(row.elevation, Some(22));
    assert_eq!(row_count(&pool, &pk).await?, 2);
    Ok(())
}

#[sqlx::test]
async fn updates_when_deployment_info_changes(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, Some(5), Some(10)).await?;
    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(7, 12))).await?;

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(7));
    assert_eq!(row.elevation, Some(12));
    assert_eq!(row_count(&pool, &pk).await?, 2);
    Ok(())
}

#[sqlx::test]
async fn noop_when_values_match(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, Some(5), Some(10)).await?;
    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(5, 10))).await?;

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(5));
    assert_eq!(row.elevation, Some(10));
    assert_eq!(row_count(&pool, &pk).await?, 1);
    Ok(())
}

#[sqlx::test]
async fn noop_when_metadata_db_has_no_row(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, Some(5), Some(10)).await?;
    // No metadata DB entry.

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(5));
    assert_eq!(row.elevation, Some(10));
    assert_eq!(row_count(&pool, &pk).await?, 1);
    Ok(())
}

#[sqlx::test]
async fn noop_when_metadata_returns_null(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, Some(5), Some(10)).await?;
    // Metadata row exists but has no deployment_info — must NOT regress
    // stored non-null values to null.
    seed_metadata_deployment(&pool, &pk, None).await?;

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(5));
    assert_eq!(row.elevation, Some(10));
    assert_eq!(row_count(&pool, &pk).await?, 1);
    Ok(())
}

#[sqlx::test]
async fn partial_update_when_only_one_field_differs(pool: PgPool) -> anyhow::Result<()> {
    gateway_metadata_db::create_tables(&pool).await;
    let pk: PublicKeyBinary = make_keypair().public_key().clone().into();

    seed_gateway(&pool, &pk, Some(5), Some(10)).await?;
    seed_metadata_deployment(&pool, &pk, Some(wifi_deployment_info(5, 12))).await?;

    tracker::execute(&pool, &pool).await?;

    let row = Gateway::get_by_address(&pool, &pk).await?.unwrap();
    assert_eq!(row.antenna, Some(5));
    assert_eq!(row.elevation, Some(12));
    assert_eq!(row_count(&pool, &pk).await?, 2);
    Ok(())
}
