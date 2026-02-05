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
pub struct HashParams {
    pub gateway_type: GatewayType,
    pub location: Option<u64>,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub location_asserts: Option<u32>,
    pub owner: Option<String>,
}

// TODO should be a part of HashParams struct?
pub fn compute_hash(params: &HashParams) -> String {
    let mut hasher = blake3::Hasher::new();

    hasher.update(params.gateway_type.to_string().as_bytes());
    hasher.update(
        params
            .location
            .map(|l| l.to_le_bytes())
            .unwrap_or([0u8; 8])
            .as_ref(),
    );
    hasher.update(
        params
            .antenna
            .map(|v| v.to_le_bytes())
            .unwrap_or([0u8; 4])
            .as_ref(),
    );
    hasher.update(
        params
            .elevation
            .map(|v| v.to_le_bytes())
            .unwrap_or([0u8; 4])
            .as_ref(),
    );
    hasher.update(
        params
            .azimuth
            .map(|v| v.to_le_bytes())
            .unwrap_or([0u8; 4])
            .as_ref(),
    );
    hasher.update(
        params
            .location_asserts
            .map(|v| v.to_le_bytes())
            .unwrap_or([0u8; 4])
            .as_ref(),
    );
    // TODO really need clone here?
    hasher.update(params.owner.clone().unwrap_or_default().as_bytes());

    hasher.finalize().to_string()
}

#[derive(Debug, Clone)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    // When the record was first created from metadata DB
    pub created_at: DateTime<Utc>,
    // When record was inserted
    pub inserted_at: DateTime<Utc>,
    // When location or hash last changed
    pub last_changed_at: DateTime<Utc>,
    pub hash: String,
    // When location last changed
    pub location_changed_at: Option<DateTime<Utc>>,
    pub owner_changed_at: Option<DateTime<Utc>>,
    pub hash_params: HashParams,
}

#[derive(Debug)]
pub struct LocationChangedAtUpdate {
    pub address: PublicKeyBinary,
    pub location_changed_at: DateTime<Utc>,
    pub location: u64,
}

impl Gateway {
    pub fn gateway_type(&self) -> GatewayType {
        self.hash_params.gateway_type
    }

    pub fn location(&self) -> Option<u64> {
        self.hash_params.location
    }

    pub fn antenna(&self) -> Option<u32> {
        self.hash_params.antenna
    }

    pub fn elevation(&self) -> Option<u32> {
        self.hash_params.elevation
    }

    pub fn azimuth(&self) -> Option<u32> {
        self.hash_params.azimuth
    }

    pub fn location_asserts(&self) -> Option<u32> {
        self.hash_params.location_asserts
    }

    // TODO rework to &String?
    pub fn owner(&self) -> Option<&str> {
        self.hash_params.owner.as_deref()
    }

    pub fn compute_hash(&self) -> String {
        compute_hash(&self.hash_params)
    }

    pub async fn insert_bulk(pool: &PgPool, rows: &[Gateway]) -> anyhow::Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            ) ",
        );

        qb.push_values(rows, |mut b, g| {
            b.push_bind(g.address.as_ref())
                .push_bind(g.hash_params.gateway_type)
                .push_bind(g.created_at)
                .push_bind(g.last_changed_at)
                .push_bind(g.hash.as_str())
                .push_bind(g.hash_params.antenna.map(|v| v as i64))
                .push_bind(g.hash_params.elevation.map(|v| v as i64))
                .push_bind(g.hash_params.azimuth.map(|v| v as i64))
                .push_bind(g.hash_params.location.map(|v| v as i64))
                .push_bind(g.location_changed_at)
                .push_bind(g.hash_params.location_asserts.map(|v| v as i64))
                .push_bind(g.hash_params.owner.as_deref())
                .push_bind(g.owner_changed_at);
        });

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
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13
            )
            "#,
        )
        .bind(self.address.as_ref())
        .bind(self.hash_params.gateway_type)
        .bind(self.created_at)
        .bind(self.last_changed_at)
        .bind(self.hash.as_str())
        .bind(self.hash_params.antenna.map(|v| v as i64))
        .bind(self.hash_params.elevation.map(|v| v as i64))
        .bind(self.hash_params.azimuth.map(|v| v as i64))
        .bind(self.hash_params.location.map(|v| v as i64))
        .bind(self.location_changed_at)
        .bind(self.hash_params.location_asserts.map(|v| v as i64))
        .bind(self.hash_params.owner.as_deref())
        .bind(self.owner_changed_at)
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
                inserted_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = $1
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(address.as_ref())
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub async fn get_by_addresses<'a>(
        db: impl PgExecutor<'a>,
        addresses: Vec<PublicKeyBinary>,
    ) -> anyhow::Result<Vec<Self>> {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        let rows = sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                inserted_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = ANY($1)
            ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(addr_array)
        .fetch_all(db)
        .await?;

        Ok(rows)
    }

    pub async fn get_by_address_and_inserted_at<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
        inserted_at_max: &DateTime<Utc>,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                gateway_type,
                created_at,
                inserted_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = $1
            AND inserted_at <= $2
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(address.as_ref())
        .bind(inserted_at_max)
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub fn stream_by_addresses<'a>(
        db: impl PgExecutor<'a> + 'a,
        addresses: Vec<PublicKeyBinary>,
        min_last_changed_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                inserted_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = ANY($1)
                AND last_changed_at >= $2
            ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(addr_array)
        .bind(min_last_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    pub fn stream_by_types<'a>(
        db: impl PgExecutor<'a> + 'a,
        types: Vec<GatewayType>,
        min_last_changed_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT DISTINCT ON (address)
                    address,
                    gateway_type,
                    created_at,
                    inserted_at,
                    last_changed_at,
                    hash,
                    antenna,
                    elevation,
                    azimuth,
                    location,
                    location_changed_at,
                    location_asserts,
                    owner,
                    owner_changed_at
                FROM gateways
                WHERE gateway_type = ANY($1)
                AND last_changed_at >= $2
                AND (
                    $3::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $3)
                )
                ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(types)
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    pub fn stream_gateway_info_v4<'a>(
        db: impl PgExecutor<'a> + 'a,
        types: Vec<GatewayType>,
        min_last_changed_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
        min_owner_changed_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT DISTINCT ON (address)
                    address,
                    gateway_type,
                    created_at,
                    inserted_at,
                    last_changed_at,
                    hash,
                    antenna,
                    elevation,
                    azimuth,
                    location,
                    location_changed_at,
                    location_asserts,
                    owner,
                    owner_changed_at
                FROM gateways
                WHERE gateway_type = ANY($1)
                AND last_changed_at >= $2
                AND (
                    $3::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $3)
                )
                AND owner_changed_at >= $4
                AND owner IS NOT NULL
                ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(types)
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .bind(min_owner_changed_at)
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
            created_at: row.try_get("created_at")?,
            inserted_at: row.try_get("inserted_at")?,
            last_changed_at: row.try_get("last_changed_at")?,
            hash: row.try_get("hash")?,
            location_changed_at: row.try_get("location_changed_at")?,
            owner_changed_at: row.try_get("owner_changed_at")?,
            hash_params: HashParams {
                gateway_type: row.try_get("gateway_type")?,
                location: to_u64(row.try_get("location")?),
                antenna: to_u32(row.try_get("antenna")?),
                elevation: to_u32(row.try_get("elevation")?),
                azimuth: to_u32(row.try_get("azimuth")?),
                location_asserts: to_u32(row.try_get("location_asserts")?),
                owner: row.try_get("owner")?,
            },
        })
    }
}
