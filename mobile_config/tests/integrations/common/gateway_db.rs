use chrono::{DateTime, Utc};
use derive_builder::Builder;
use helium_crypto::{PublicKey, PublicKeyBinary};
use mobile_config::gateway::db::{Gateway, GatewayType};
use sqlx::PgPool;

/// Builder mirroring the old `gateways`-table test gateway. `inserted_at` and
/// `hash` are retained as ignored fields so existing call sites keep compiling;
/// the dbt-backed schema has no equivalents.
#[derive(Builder)]
#[builder(setter(into))]
pub struct TestGateway {
    pub address: PublicKey,
    pub gateway_type: GatewayType,
    #[builder(default)]
    created_at: DateTime<Utc>,
    #[builder(default)]
    #[allow(dead_code)]
    inserted_at: DateTime<Utc>,
    #[builder(default)]
    last_changed_at: DateTime<Utc>,
    #[builder(default)]
    #[allow(dead_code)]
    hash: String,
    #[builder(default = "Some(18)")]
    antenna: Option<u32>,
    #[builder(default)]
    elevation: Option<u32>,
    #[builder(default)]
    azimuth: Option<u32>,
    #[builder(default)]
    location: Option<u64>,
    #[builder(default)]
    location_changed_at: Option<DateTime<Utc>>,
    #[builder(default)]
    location_asserts: Option<u32>,
    #[builder(default)]
    owner: Option<String>,
    #[builder(default)]
    owner_changed_at: Option<DateTime<Utc>>,
}

impl From<TestGateway> for Gateway {
    fn from(tg: TestGateway) -> Self {
        Gateway {
            address: tg.address.into(),
            gateway_type: tg.gateway_type,
            created_at: tg.created_at,
            last_changed_at: tg.last_changed_at,
            antenna: tg.antenna,
            elevation: tg.elevation,
            azimuth: tg.azimuth,
            location: tg.location,
            location_changed_at: tg.location_changed_at,
            location_asserts: tg.location_asserts,
            owner: tg.owner,
            owner_changed_at: tg.owner_changed_at,
        }
    }
}

/// Create the dbt-owned chain tables that gateway reads target. dbt maintains
/// these in prod; tests must stand them up themselves. Idempotent.
pub async fn create_dbt_tables(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS dbt")
        .execute(pool)
        .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS dbt.mobile_gateway_inventory (
            address             varchar PRIMARY KEY,
            device_type         varchar,
            serial_number       varchar,
            name                varchar,
            asset               varchar,
            location_hex        varchar,
            azimuth             integer,
            created_at          timestamptz,
            config_updated_at   timestamptz,
            updated_at          timestamptz,
            location_changed_at timestamptz,
            location_asserts    bigint,
            owner               varchar,
            owner_changed_at    timestamptz
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS dbt.mobile_hotspot_history (
            file_ts            bigint,
            record_index       bigint,
            received_timestamp timestamptz,
            block              bigint,
            "timestamp"        timestamptz,
            pub_key            varchar,
            asset              varchar,
            serial_number      varchar,
            device_type        varchar,
            asserted_hex       varchar,
            azimuth            integer,
            signer             varchar,
            animal_name        varchar
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn location_hex(location: Option<u64>) -> Option<String> {
    location.map(|l| format!("{l:x}"))
}

/// Test-only extension: persist a `Gateway` into the dbt-shaped tables plus the
/// local `deployment_info` cache, so the read path sees it.
#[allow(async_fn_in_trait)]
pub trait GatewayTestExt {
    async fn insert(&self, pool: &PgPool) -> anyhow::Result<()>;
}

impl GatewayTestExt for Gateway {
    async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        create_dbt_tables(pool).await?;

        sqlx::query(
            r#"
            INSERT INTO dbt.mobile_gateway_inventory (
                address, device_type, location_hex, azimuth,
                created_at, config_updated_at, updated_at,
                location_changed_at, location_asserts, owner, owner_changed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, $10)
            ON CONFLICT (address) DO UPDATE SET
                device_type = EXCLUDED.device_type,
                location_hex = EXCLUDED.location_hex,
                azimuth = EXCLUDED.azimuth,
                created_at = EXCLUDED.created_at,
                config_updated_at = EXCLUDED.config_updated_at,
                updated_at = EXCLUDED.updated_at,
                location_changed_at = EXCLUDED.location_changed_at,
                location_asserts = EXCLUDED.location_asserts,
                owner = EXCLUDED.owner,
                owner_changed_at = EXCLUDED.owner_changed_at
            "#,
        )
        .bind(self.address.to_string())
        .bind(self.gateway_type.as_dbt_str())
        .bind(location_hex(self.location))
        .bind(self.azimuth.map(|v| v as i32))
        .bind(self.created_at)
        // $6 fills both config_updated_at and updated_at (last observable change)
        .bind(self.last_changed_at)
        .bind(self.location_changed_at)
        .bind(self.location_asserts.map(|v| v as i64))
        .bind(self.owner.as_deref())
        .bind(self.owner_changed_at)
        .execute(pool)
        .await?;

        if self.antenna.is_some() || self.elevation.is_some() {
            set_deployment_info(pool, &self.address, self.antenna, self.elevation).await?;
        }

        // Append a matching history version so point-in-time reads work.
        insert_history_version(
            pool,
            &self.address,
            self.gateway_type,
            self.last_changed_at,
            self.location,
            self.azimuth,
        )
        .await?;

        Ok(())
    }
}

/// Upsert an antenna/elevation pair into the local deployment_info cache.
pub async fn set_deployment_info(
    pool: &PgPool,
    address: &PublicKeyBinary,
    antenna: Option<u32>,
    elevation: Option<u32>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO deployment_info (address, antenna, elevation, refreshed_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (address) DO UPDATE SET
            antenna = EXCLUDED.antenna,
            elevation = EXCLUDED.elevation,
            refreshed_at = now()
        "#,
    )
    .bind(address.to_string())
    .bind(antenna.map(|v| v as i32))
    .bind(elevation.map(|v| v as i32))
    .execute(pool)
    .await?;
    Ok(())
}

/// Append one row to the dbt history table (point-in-time source).
pub async fn insert_history_version(
    pool: &PgPool,
    address: &PublicKeyBinary,
    gateway_type: GatewayType,
    timestamp: DateTime<Utc>,
    location: Option<u64>,
    azimuth: Option<u32>,
) -> anyhow::Result<()> {
    create_dbt_tables(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO dbt.mobile_hotspot_history (
            file_ts, record_index, received_timestamp, block,
            "timestamp", pub_key, device_type, asserted_hex, azimuth
        )
        VALUES (
            extract(epoch from $4)::bigint, 0, $4, 0,
            $4, $1, $2, $3, $5
        )
        "#,
    )
    .bind(address.to_string())
    .bind(gateway_type.as_dbt_str())
    .bind(location_hex(location))
    .bind(timestamp)
    .bind(azimuth.map(|v| v as i32))
    .execute(pool)
    .await?;
    Ok(())
}
