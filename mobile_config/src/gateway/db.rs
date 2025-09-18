use crate::gateway::service::{info::DeviceType, info_v3::DeviceTypeV2};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{postgres::PgRow, FromRow, PgExecutor, PgPool, Postgres, QueryBuilder, Row};
use std::convert::TryFrom;
use strum::EnumIter;

// Postgres enum: gateway_type
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, EnumIter)]
#[sqlx(type_name = "gateway_type")]
pub enum GatewayType {
    #[sqlx(rename = "wifiIndoor")]
    WifiIndoor,
    #[sqlx(rename = "wifiOutdoor")]
    WifiOutdoor,
    #[sqlx(rename = "wifiDataOnly")]
    WifiDataOnly,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid device type string")]
pub struct GatewayTypeParseError;

impl std::fmt::Display for GatewayType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayType::WifiIndoor => write!(f, "wifiIndoor"),
            GatewayType::WifiOutdoor => write!(f, "wifiOutdoor"),
            GatewayType::WifiDataOnly => write!(f, "wifiDataOnly"),
        }
    }
}

impl std::str::FromStr for GatewayType {
    type Err = GatewayTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "wifiIndoor" => Self::WifiIndoor,
            "wifiOutdoor" => Self::WifiOutdoor,
            "wifiDataOnly" => Self::WifiDataOnly,
            _ => return Err(GatewayTypeParseError),
        };
        Ok(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayTypeError {
    #[error("CBRS gateways are not supported in the Gateway table")]
    CbrsUnsupported,
}

impl TryFrom<DeviceType> for GatewayType {
    type Error = GatewayTypeError;

    fn try_from(dt: DeviceType) -> Result<Self, Self::Error> {
        match dt {
            DeviceType::Cbrs => Err(GatewayTypeError::CbrsUnsupported),
            DeviceType::WifiIndoor => Ok(GatewayType::WifiIndoor),
            DeviceType::WifiOutdoor => Ok(GatewayType::WifiOutdoor),
            DeviceType::WifiDataOnly => Ok(GatewayType::WifiDataOnly),
        }
    }
}

impl From<DeviceTypeV2> for GatewayType {
    fn from(dt: DeviceTypeV2) -> Self {
        match dt {
            DeviceTypeV2::Indoor => GatewayType::WifiIndoor,
            DeviceTypeV2::Outdoor => GatewayType::WifiOutdoor,
            DeviceTypeV2::DataOnly => GatewayType::WifiDataOnly,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    pub gateway_type: GatewayType,
    // When the record was first created from metadata DB
    pub created_at: DateTime<Utc>,
    // When record was last updated
    pub updated_at: DateTime<Utc>,
    // When record was last updated from metadata DB (could be set to now if no metadata DB info)
    pub refreshed_at: DateTime<Utc>,
    // When location or hash last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub last_changed_at: DateTime<Utc>,
    pub hash: String,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub location: Option<u64>,
    // When location last changed, set to refreshed_at (updated via SQL query see Gateway::insert)
    pub location_changed_at: Option<DateTime<Utc>>,
    pub location_asserts: Option<u32>,
}

impl Gateway {
    pub async fn insert_bulk(pool: &PgPool, rows: &[Gateway]) -> anyhow::Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                updated_at,
                refreshed_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts
            ) ",
        );

        qb.push_values(rows, |mut b, g| {
            b.push_bind(g.address.as_ref())
                .push_bind(g.gateway_type)
                .push_bind(g.created_at)
                .push_bind(g.updated_at)
                .push_bind(g.refreshed_at)
                .push_bind(g.last_changed_at)
                .push_bind(g.hash.as_str())
                .push_bind(g.antenna.map(|v| v as i64))
                .push_bind(g.elevation.map(|v| v as i64))
                .push_bind(g.azimuth.map(|v| v as i64))
                .push_bind(g.location.map(|v| v as i64))
                .push_bind(g.location_changed_at)
                .push_bind(g.location_asserts.map(|v| v as i64));
        });

        qb.push(
            " ON CONFLICT (address) DO UPDATE SET 
                gateway_type = EXCLUDED.gateway_type,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                refreshed_at = EXCLUDED.refreshed_at,
                last_changed_at = CASE 
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location 
                      OR gateways.hash     IS DISTINCT FROM EXCLUDED.hash 
                    THEN EXCLUDED.refreshed_at 
                    ELSE gateways.last_changed_at 
                END,
                hash = EXCLUDED.hash,
                antenna = EXCLUDED.antenna,
                elevation = EXCLUDED.elevation,
                azimuth = EXCLUDED.azimuth,
                location = EXCLUDED.location,
                location_changed_at = CASE 
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location 
                    THEN EXCLUDED.refreshed_at 
                    ELSE gateways.location_changed_at 
                END,
                location_asserts = EXCLUDED.location_asserts",
        );

        let res = qb.build().execute(pool).await?;
        Ok(res.rows_affected())
    }

    pub async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                updated_at,
                refreshed_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (address)
            DO UPDATE SET
                gateway_type = EXCLUDED.gateway_type,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                refreshed_at = EXCLUDED.refreshed_at,
                last_changed_at = CASE
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location
                        OR gateways.hash IS DISTINCT FROM EXCLUDED.hash
                    THEN EXCLUDED.refreshed_at
                    ELSE gateways.last_changed_at
                END,
                hash = EXCLUDED.hash,
                antenna = EXCLUDED.antenna,
                elevation = EXCLUDED.elevation,
                azimuth = EXCLUDED.azimuth,
                location = EXCLUDED.location,
                location_changed_at = CASE
                    WHEN gateways.location IS DISTINCT FROM EXCLUDED.location
                    THEN EXCLUDED.refreshed_at
                    ELSE gateways.location_changed_at
                END,
                location_asserts = EXCLUDED.location_asserts
            "#,
        )
        .bind(self.address.as_ref())
        .bind(self.gateway_type)
        .bind(self.created_at)
        .bind(self.updated_at)
        .bind(self.refreshed_at)
        .bind(self.last_changed_at)
        .bind(self.hash.as_str())
        .bind(self.antenna.map(|v| v as i64))
        .bind(self.elevation.map(|v| v as i64))
        .bind(self.azimuth.map(|v| v as i64))
        .bind(self.location.map(|v| v as i64))
        .bind(self.location_changed_at)
        .bind(self.location_asserts.map(|v| v as i64))
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_by_address<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                gateway_type,
                created_at,
                updated_at,
                refreshed_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts
            FROM gateways
            WHERE address = $1
            "#,
        )
        .bind(address.as_ref())
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub fn stream_by_addresses<'a>(
        db: impl PgExecutor<'a> + 'a,
        addresses: Vec<PublicKeyBinary>,
        min_updated_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                gateway_type,
                created_at,
                updated_at,
                refreshed_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts
            FROM gateways
            WHERE address = ANY($1)
                AND (updated_at >= $2 OR refreshed_at >= $2 OR created_at >= $2)
            "#,
        )
        .bind(addr_array)
        .bind(min_updated_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    pub fn stream_by_types<'a>(
        db: impl PgExecutor<'a> + 'a,
        types: Vec<GatewayType>,
        min_date: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT
                    address,
                    gateway_type,
                    created_at,
                    updated_at,
                    refreshed_at,
                    last_changed_at,
                    hash,
                    antenna,
                    elevation,
                    azimuth,
                    location,
                    location_changed_at,
                    location_asserts
                FROM gateways
                WHERE gateway_type = ANY($1)
                AND (updated_at >= $2 OR refreshed_at >= $2 OR created_at >= $2)
                AND (
                    $3::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $3)
                )
            "#,
        )
        .bind(types)
        .bind(min_date)
        .bind(min_location_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }
}

impl FromRow<'_, PgRow> for Gateway {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        // helpers to map Option<i64> -> Option<u32/u64>
        let to_u32 = |v: Option<i64>| -> Option<u32> { v.map(|x| x as u32) };
        let to_u64 = |v: Option<i64>| -> Option<u64> { v.map(|x| x as u64) };

        Ok(Self {
            address: PublicKeyBinary::from(row.try_get::<Vec<u8>, _>("address")?),
            gateway_type: row.try_get("gateway_type")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            refreshed_at: row.try_get("refreshed_at")?,
            last_changed_at: row.try_get("last_changed_at")?,
            hash: row.try_get("hash")?,
            antenna: to_u32(row.try_get("antenna")?),
            elevation: to_u32(row.try_get("elevation")?),
            azimuth: to_u32(row.try_get("azimuth")?),
            location: to_u64(row.try_get("location")?),
            location_changed_at: row.try_get("location_changed_at")?,
            location_asserts: to_u32(row.try_get("location_asserts")?),
        })
    }
}
