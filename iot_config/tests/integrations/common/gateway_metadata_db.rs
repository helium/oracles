use bs58;
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;

#[allow(clippy::too_many_arguments)]
pub async fn insert_gateway(
    pool: &PgPool,
    address: &str,
    asset: &str,
    location: Option<i64>,
    elevation: Option<i64>,
    gain: Option<i64>,
    is_full_hotspot: Option<bool>,
    num_location_asserts: Option<i32>,
    is_active: Option<bool>,
    dc_onboarding_fee_paid: Option<i64>,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
    last_block: Option<i64>,
    key: PublicKeyBinary,
) -> anyhow::Result<()> {
    insert_iot_hotspot_infos(
        pool,
        address,
        asset,
        location,
        elevation,
        gain,
        is_full_hotspot,
        num_location_asserts,
        is_active,
        dc_onboarding_fee_paid,
        created_at,
        refreshed_at,
        last_block,
    )
    .await?;

    insert_asset_key(pool, asset, key).await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_iot_hotspot_infos(
    pool: &PgPool,
    address: &str,
    asset: &str,
    location: Option<i64>,
    elevation: Option<i64>,
    gain: Option<i64>,
    is_full_hotspot: Option<bool>,
    num_location_asserts: Option<i32>,
    is_active: Option<bool>,
    dc_onboarding_fee_paid: Option<i64>,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
    last_block: Option<i64>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO iot_hotspot_infos (
            address,
            asset,
            location,
            elevation,
            gain,
            is_full_hotspot,
            num_location_asserts,
            is_active,
            dc_onboarding_fee_paid,
            created_at,
            refreshed_at,
            last_block
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, 0)
        )
        ON CONFLICT (address)
        DO UPDATE SET
            asset = EXCLUDED.asset,
            location = EXCLUDED.location,
            elevation = EXCLUDED.elevation,
            gain = EXCLUDED.gain,
            is_full_hotspot = EXCLUDED.is_full_hotspot,
            num_location_asserts = EXCLUDED.num_location_asserts,
            is_active = EXCLUDED.is_active,
            dc_onboarding_fee_paid = EXCLUDED.dc_onboarding_fee_paid,
            refreshed_at = EXCLUDED.refreshed_at,
            last_block = EXCLUDED.last_block;
        "#,
    )
    .bind(address)
    .bind(asset)
    .bind(location)
    .bind(elevation)
    .bind(gain)
    .bind(is_full_hotspot)
    .bind(num_location_asserts)
    .bind(is_active)
    .bind(dc_onboarding_fee_paid)
    .bind(created_at)
    .bind(refreshed_at)
    .bind(last_block)
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_asset_key(pool: &PgPool, asset: &str, key: PublicKeyBinary) -> anyhow::Result<()> {
    let b58 = bs58::decode(key.to_string()).into_vec().unwrap();
    sqlx::query(
        r#"
        INSERT INTO
            "key_to_assets" ("asset", "entity_key")
        VALUES ($1, $2);
        "#,
    )
    .bind(asset)
    .bind(b58)
    .execute(pool)
    .await?;

    Ok(())
}

/// Insert a gateway with an invalid entity_key (for testing error handling)
#[allow(clippy::too_many_arguments)]
pub async fn insert_gateway_with_invalid_key(
    pool: &PgPool,
    address: &str,
    asset: &str,
    location: Option<i64>,
    elevation: Option<i64>,
    gain: Option<i64>,
    is_full_hotspot: Option<bool>,
    num_location_asserts: Option<i32>,
    is_active: Option<bool>,
    dc_onboarding_fee_paid: Option<i64>,
    created_at: DateTime<Utc>,
    refreshed_at: Option<DateTime<Utc>>,
    last_block: Option<i64>,
    invalid_entity_key_bytes: Vec<u8>,
) -> anyhow::Result<()> {
    insert_iot_hotspot_infos(
        pool,
        address,
        asset,
        location,
        elevation,
        gain,
        is_full_hotspot,
        num_location_asserts,
        is_active,
        dc_onboarding_fee_paid,
        created_at,
        refreshed_at,
        last_block,
    )
    .await?;

    // Insert the invalid key directly as bytes
    sqlx::query(
        r#"
        INSERT INTO
            "key_to_assets" ("asset", "entity_key")
        VALUES ($1, $2);
        "#,
    )
    .bind(asset)
    .bind(invalid_entity_key_bytes)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn create_tables(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS iot_hotspot_infos (
            address                VARCHAR(255) NOT NULL,
            asset                  VARCHAR(255),
            bump_seed              INTEGER,
            location               NUMERIC,
            elevation              NUMERIC,
            gain                   NUMERIC,
            is_full_hotspot        BOOLEAN,
            num_location_asserts   INTEGER,
            is_active              BOOLEAN,
            dc_onboarding_fee_paid NUMERIC,
            refreshed_at           TIMESTAMPTZ,
            created_at             TIMESTAMPTZ NOT NULL,
            last_block             NUMERIC NOT NULL DEFAULT 0,
            PRIMARY KEY (address)
        );"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE key_to_assets (
            asset character varying(255) NULL,
            entity_key bytea NULL
        );"#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
