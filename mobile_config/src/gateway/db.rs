use crate::gateway::service::{info::DeviceType, info_v3::DeviceTypeV2};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{postgres::PgRow, FromRow, PgExecutor, Row};
use std::{convert::TryFrom, str::FromStr};
use strum::EnumIter;

/// Builds a current-state read query with the given WHERE clause.
/// `dbt.mobile_gateway_inventory` is already one row per gateway;
/// `antenna`/`elevation` are joined from the local `deployment_info` cache
/// (they are not carried by the dbt chain tables). Expands to a `&'static str`
/// so the streaming queries can return borrowing the query safely.
macro_rules! gateway_query {
    ($where:literal) => {
        concat!(
            "SELECT ",
            "g.address, g.device_type, g.created_at, g.updated_at, ",
            "g.location_hex, g.azimuth, g.location_changed_at, g.location_asserts, ",
            "g.owner, g.owner_changed_at, d.antenna, d.elevation ",
            "FROM dbt.mobile_gateway_inventory g ",
            "LEFT JOIN deployment_info d ON d.address = g.address ",
            "WHERE ",
            $where
        )
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum GatewayType {
    WifiIndoor,
    WifiOutdoor,
    WifiDataOnly,
}

impl GatewayType {
    /// String form stored in `dbt.mobile_gateway_inventory.device_type` and
    /// `dbt.mobile_hotspot_history.device_type` (screaming snake case).
    pub fn as_dbt_str(&self) -> &'static str {
        match self {
            GatewayType::WifiIndoor => "WIFI_INDOOR",
            GatewayType::WifiOutdoor => "WIFI_OUTDOOR",
            GatewayType::WifiDataOnly => "WIFI_DATA_ONLY",
        }
    }

    pub fn from_dbt_str(s: &str) -> Result<Self, GatewayTypeParseError> {
        match s {
            "WIFI_INDOOR" => Ok(GatewayType::WifiIndoor),
            "WIFI_OUTDOOR" => Ok(GatewayType::WifiOutdoor),
            "WIFI_DATA_ONLY" => Ok(GatewayType::WifiDataOnly),
            _ => Err(GatewayTypeParseError),
        }
    }
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

/// Read model shared by all gateway RPCs. Current-state reads come from
/// `dbt.mobile_gateway_inventory` (joined to `deployment_info`); the
/// point-in-time `get_by_address_as_of` read comes from
/// `dbt.mobile_hotspot_history`.
#[derive(Debug, Clone)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    pub gateway_type: GatewayType,
    // First time the entity was seen (min history timestamp).
    pub created_at: DateTime<Utc>,
    // Last time any observable field changed (dbt `updated_at`).
    pub last_changed_at: DateTime<Utc>,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub location: Option<u64>,
    pub location_changed_at: Option<DateTime<Utc>>,
    pub location_asserts: Option<u32>,
    pub owner: Option<String>,
    pub owner_changed_at: Option<DateTime<Utc>>,
}

impl Gateway {
    pub async fn get_by_address<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(gateway_query!("g.address = $1"))
            .bind(address.to_string())
            .fetch_optional(db)
            .await?;

        Ok(gateway)
    }

    pub async fn get_by_addresses<'a>(
        db: impl PgExecutor<'a>,
        addresses: Vec<PublicKeyBinary>,
    ) -> anyhow::Result<Vec<Self>> {
        let addr_array: Vec<String> = addresses.iter().map(|a| a.to_string()).collect();

        let rows = sqlx::query_as::<_, Self>(gateway_query!("g.address = ANY($1)"))
            .bind(addr_array)
            .fetch_all(db)
            .await?;

        Ok(rows)
    }

    /// Point-in-time read: the latest history version at or before
    /// `as_of`. Serves `info_at_timestamp`. `antenna`/`elevation` reflect the
    /// current deployment (the metadata DB has no history for them), matching
    /// prior behavior.
    pub async fn get_by_address_as_of<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
        as_of: &DateTime<Utc>,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                h.pub_key           AS address,
                h.device_type       AS device_type,
                lt.created_at       AS created_at,
                h."timestamp"       AS updated_at,
                h.asserted_hex      AS location_hex,
                h.azimuth           AS azimuth,
                NULL::timestamptz   AS location_changed_at,
                NULL::bigint        AS location_asserts,
                NULL::text          AS owner,
                NULL::timestamptz   AS owner_changed_at,
                d.antenna           AS antenna,
                d.elevation         AS elevation
            FROM dbt.mobile_hotspot_history h
            CROSS JOIN (
                SELECT min("timestamp") AS created_at
                FROM dbt.mobile_hotspot_history
                WHERE pub_key = $1
            ) lt
            LEFT JOIN deployment_info d ON d.address = h.pub_key
            WHERE h.pub_key = $1
                AND h."timestamp" <= $2
            ORDER BY h."timestamp" DESC, h.block DESC, h.record_index DESC
            LIMIT 1
            "#,
        )
        .bind(address.to_string())
        .bind(as_of)
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub fn stream_by_addresses<'a>(
        db: impl PgExecutor<'a> + 'a,
        addresses: Vec<PublicKeyBinary>,
        min_last_changed_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        let addr_array: Vec<String> = addresses.iter().map(|a| a.to_string()).collect();

        sqlx::query_as::<_, Self>(gateway_query!("g.address = ANY($1) AND g.updated_at >= $2"))
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
        let type_array: Vec<String> = types.iter().map(|t| t.as_dbt_str().to_string()).collect();

        sqlx::query_as::<_, Self>(gateway_query!(
            "g.device_type = ANY($1) \
             AND g.updated_at >= $2 \
             AND ( \
                 $3::timestamptz IS NULL \
                 OR (g.location_hex IS NOT NULL AND g.location_changed_at >= $3) \
             )"
        ))
        .bind(type_array)
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
        let type_array: Vec<String> = types.iter().map(|t| t.as_dbt_str().to_string()).collect();

        sqlx::query_as::<_, Self>(gateway_query!(
            "g.device_type = ANY($1) \
             AND g.updated_at >= $2 \
             AND ( \
                 $3::timestamptz IS NULL \
                 OR (g.location_hex IS NOT NULL AND g.location_changed_at >= $3) \
             ) \
             AND g.owner_changed_at >= $4 \
             AND g.owner IS NOT NULL"
        ))
        .bind(type_array)
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .bind(min_owner_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }
}

/// Parse an H3 cell from the stored hex string. `None` for an empty/`0x`-only
/// or unparseable value.
fn parse_location_hex(hex: &str) -> Option<u64> {
    let trimmed = hex.trim_start_matches("0x");
    if trimmed.is_empty() {
        return None;
    }
    u64::from_str_radix(trimmed, 16).ok()
}

impl FromRow<'_, PgRow> for Gateway {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let to_u32 = |v: Option<i64>| -> Option<u32> { v.map(|x| x as u32) };

        let address = PublicKeyBinary::from_str(&row.try_get::<String, _>("address")?)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let gateway_type = GatewayType::from_dbt_str(&row.try_get::<String, _>("device_type")?)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let location = row
            .try_get::<Option<String>, _>("location_hex")?
            .as_deref()
            .and_then(parse_location_hex);

        Ok(Self {
            address,
            gateway_type,
            created_at: row.try_get("created_at")?,
            last_changed_at: row.try_get("updated_at")?,
            antenna: to_u32(row.try_get::<Option<i32>, _>("antenna")?.map(|v| v as i64)),
            elevation: to_u32(
                row.try_get::<Option<i32>, _>("elevation")?
                    .map(|v| v as i64),
            ),
            azimuth: to_u32(row.try_get::<Option<i32>, _>("azimuth")?.map(|v| v as i64)),
            location,
            location_changed_at: row.try_get("location_changed_at")?,
            location_asserts: to_u32(row.try_get("location_asserts")?),
            owner: row.try_get("owner")?,
            owner_changed_at: row.try_get("owner_changed_at")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_location_hex_plain() {
        assert_eq!(
            parse_location_hex("8528347ffffffff"),
            Some(0x8528347ffffffff)
        );
    }

    #[test]
    fn parse_location_hex_with_0x_prefix() {
        assert_eq!(
            parse_location_hex("0x8528347ffffffff"),
            Some(0x8528347ffffffff)
        );
    }

    #[test]
    fn parse_location_hex_rejects_empty() {
        assert_eq!(parse_location_hex(""), None);
        assert_eq!(parse_location_hex("0x"), None);
    }

    #[test]
    fn parse_location_hex_rejects_malformed() {
        assert_eq!(parse_location_hex("not-hex"), None);
    }

    #[test]
    fn gateway_type_dbt_str_round_trip() {
        for gt in [
            GatewayType::WifiIndoor,
            GatewayType::WifiOutdoor,
            GatewayType::WifiDataOnly,
        ] {
            assert_eq!(GatewayType::from_dbt_str(gt.as_dbt_str()).unwrap(), gt);
        }
    }

    #[test]
    fn gateway_type_from_dbt_str_matches_table_values() {
        assert_eq!(
            GatewayType::from_dbt_str("WIFI_INDOOR").unwrap(),
            GatewayType::WifiIndoor
        );
        assert_eq!(
            GatewayType::from_dbt_str("WIFI_OUTDOOR").unwrap(),
            GatewayType::WifiOutdoor
        );
        assert_eq!(
            GatewayType::from_dbt_str("WIFI_DATA_ONLY").unwrap(),
            GatewayType::WifiDataOnly
        );
    }

    #[test]
    fn gateway_type_from_dbt_str_rejects_unknown() {
        // CBRS is excluded from the inventory; lowercase legacy forms must not match.
        assert!(GatewayType::from_dbt_str("CBRS").is_err());
        assert!(GatewayType::from_dbt_str("wifiIndoor").is_err());
    }
}
