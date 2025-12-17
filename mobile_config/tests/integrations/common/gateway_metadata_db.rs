use bs58;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{PgPool, Postgres, QueryBuilder};

pub struct GatewayInsert {
    pub asset: String,
    pub location: Option<i64>,
    pub device_type: String,
    pub key: PublicKeyBinary,
    pub created_at: DateTime<Utc>,
    pub refreshed_at: Option<DateTime<Utc>>,
    pub deployment_info: Option<String>,
}

pub async fn insert_gateway_bulk(
    pool: &PgPool,
    gateways: &[GatewayInsert],
    chunk_size: usize,
) -> anyhow::Result<()> {
    stream::iter(gateways.chunks(chunk_size))
        .map(Ok) // convert chunks to a Result for try_for_each_concurrent
        .try_for_each_concurrent(20, |chunk| {
            let pool = pool.clone();
            async move {
                // insert mobile_hotspot_infos
                let mut qb = QueryBuilder::<Postgres>::new(
                    r#"
                    INSERT INTO mobile_hotspot_infos (
                        asset, location, device_type, created_at,
                        refreshed_at, deployment_info, num_location_asserts
                    )
                    "#,
                );

                qb.push_values(chunk, |mut b, gw| {
                    let num_locations = if gw.location.is_some() {
                        Some(1)
                    } else {
                        Some(0)
                    };

                    let device_type_json: serde_json::Value =
                        serde_json::from_str(&gw.device_type).unwrap();
                    let deployment_info_json: serde_json::Value =
                        serde_json::from_str(gw.deployment_info.as_deref().unwrap_or("null"))
                            .unwrap();

                    b.push_bind(&gw.asset)
                        .push_bind(gw.location)
                        .push_bind(device_type_json)
                        .push_bind(gw.created_at)
                        .push_bind(gw.refreshed_at)
                        .push_bind(deployment_info_json)
                        .push_bind(num_locations);
                });

                qb.build().execute(&pool).await?;

                // insert key_to_assets
                let mut qb1 = QueryBuilder::<Postgres>::new(
                    r#"
                    INSERT INTO key_to_assets (
                        asset, entity_key
                    )
                    "#,
                );

                qb1.push_values(chunk, |mut b, gw| {
                    let b58 = bs58::decode(gw.key.to_string()).into_vec().unwrap();
                    b.push_bind(&gw.asset).push_bind(b58);
                });

                qb1.build().execute(&pool).await?;

                Ok::<_, anyhow::Error>(())
            }
        })
        .await?;

    Ok(())
}

pub async fn update_gateway(
    pool: &PgPool,
    asset: &str,
    location: i64,
    refreshed_at: DateTime<Utc>,
    num_location_asserts: i32,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE mobile_hotspot_infos
        SET location = $1,
            num_location_asserts = $2,
            refreshed_at = $3
        WHERE asset = $4
        "#,
    )
    .bind(location)
    .bind(num_location_asserts)
    .bind(refreshed_at)
    .bind(asset)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_asset_owner(
    pool: &PgPool,
    asset: &str,
    owner: &str,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO asset_owners (
            asset, owner, created_at, updated_at, last_block
        )
        VALUES ($1, $2, $3, $4, 0)
        "#,
    )
    .bind(asset)
    .bind(owner)
    .bind(created_at)
    .bind(updated_at)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn update_asset_owner(
    pool: &PgPool,
    asset: &str,
    owner: &str,
    updated_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE asset_owners
        SET owner = $1,
            updated_at = $2
        WHERE asset = $3
        "#,
    )
    .bind(owner)
    .bind(updated_at)
    .bind(asset)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn create_tables(pool: &PgPool) {
    sqlx::query(
        r#"
        CREATE TABLE mobile_hotspot_infos (
            asset character varying(255) NULL,
            location numeric NULL,
            device_type jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            refreshed_at timestamptz,
            deployment_info jsonb,
            is_full_hotspot bool NULL,
            num_location_asserts integer NULL,
            is_active bool NULL,
            dc_onboarding_fee_paid numeric NULL
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
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE asset_owners (
            asset character varying(255) NULL,
            owner character varying(255) NULL,
            created_at timestamptz,
            updated_at timestamptz,
            last_block integer
        );"#,
    )
    .execute(pool)
    .await
    .unwrap();
}
