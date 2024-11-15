use chrono::{DateTime, TimeZone, Utc};
use futures::stream::BoxStream;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    DeviceType as DeviceTypeProto, GatewayInfo as GatewayInfoProto,
    GatewayMetadata as GatewayMetadataProto,
};

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

#[derive(Clone, Debug)]
pub struct GatewayMetadata {
    pub location: u64,
}

#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadata>,
    pub device_type: DeviceType,
    pub refreshed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl GatewayInfo {
    pub fn is_data_only(&self) -> bool {
        matches!(self.device_type, DeviceType::WifiDataOnly)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GatewayInfoProtoParseError {
    #[error("Invalid location string: {0}")]
    InvalidLocation(#[from] std::num::ParseIntError),
    #[error("Invalid created_at: {0}")]
    InvalidCreatedAt(u64),
    #[error("Invalid refreshed_at: {0}")]
    InvalidRefreshedAt(u64),
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = GatewayInfoProtoParseError;

    fn try_from(info: GatewayInfoProto) -> Result<Self, Self::Error> {
        let metadata = if let Some(ref metadata) = info.metadata {
            Some(
                u64::from_str_radix(&metadata.location, 16)
                    .map(|location| GatewayMetadata { location })?,
            )
        } else {
            None
        };
        let device_type = info.device_type().into();

        let created_at = Utc
            .timestamp_opt(info.created_at as i64, 0)
            .single()
            .ok_or(GatewayInfoProtoParseError::InvalidCreatedAt(
                info.created_at,
            ))?;

        let refreshed_at = Utc
            .timestamp_opt(info.refreshed_at as i64, 0)
            .single()
            .ok_or(GatewayInfoProtoParseError::InvalidRefreshedAt(
                info.refreshed_at,
            ))?;

        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type,
            created_at,
            refreshed_at,
        })
    }
}

impl TryFrom<GatewayInfo> for GatewayInfoProto {
    type Error = hextree::Error;

    fn try_from(info: GatewayInfo) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            Some(GatewayMetadataProto {
                location: hextree::Cell::from_raw(metadata.location)?.to_string(),
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type: info.device_type as i32,
            created_at: info.created_at.timestamp() as u64,
            refreshed_at: info.created_at.timestamp() as u64,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum DeviceType {
    Cbrs,
    WifiIndoor,
    WifiOutdoor,
    WifiDataOnly,
}

impl From<DeviceTypeProto> for DeviceType {
    fn from(dtp: DeviceTypeProto) -> Self {
        match dtp {
            DeviceTypeProto::Cbrs => DeviceType::Cbrs,
            DeviceTypeProto::WifiIndoor => DeviceType::WifiIndoor,
            DeviceTypeProto::WifiOutdoor => DeviceType::WifiOutdoor,
            DeviceTypeProto::WifiDataOnly => DeviceType::WifiDataOnly,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid device type string")]
pub struct DeviceTypeParseError;

impl std::fmt::Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceType::Cbrs => write!(f, "cbrs"),
            DeviceType::WifiIndoor => write!(f, "wifiIndoor"),
            DeviceType::WifiOutdoor => write!(f, "wifiOutdoor"),
            DeviceType::WifiDataOnly => write!(f, "wifiDataOnly"),
        }
    }
}
impl std::str::FromStr for DeviceType {
    type Err = DeviceTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "cbrs" => Self::Cbrs,
            "wifiIndoor" => Self::WifiIndoor,
            "wifiOutdoor" => Self::WifiOutdoor,
            "wifiDataOnly" => Self::WifiDataOnly,
            _ => return Err(DeviceTypeParseError),
        };
        Ok(result)
    }
}

pub(crate) mod db {
    use super::{DeviceType, GatewayInfo, GatewayMetadata};
    use chrono::{DateTime, Utc};
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{types::Json, PgExecutor, Row};
    use std::str::FromStr;

    const GET_METADATA_SQL: &str = r#"
            select kta.entity_key, infos.location::bigint, infos.device_type,
                infos.refreshed_at, infos.created_at
            from mobile_hotspot_infos infos
            join key_to_assets kta on infos.asset = kta.asset
        "#;
    const BATCH_SQL_WHERE_SNIPPET: &str = " where kta.entity_key = any($1::bytea[]) ";

    lazy_static::lazy_static! {
        static ref BATCH_METADATA_SQL: String = format!("{GET_METADATA_SQL} {BATCH_SQL_WHERE_SNIPPET}");
    }

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<GatewayInfo>> {
        let entity_key = bs58::decode(address.to_string()).into_vec()?;
        let mut query: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(GET_METADATA_SQL);
        query.push(" where kta.entity_key = $1 ");
        Ok(query
            .build_query_as::<GatewayInfo>()
            .bind(entity_key)
            .fetch_optional(db)
            .await?)
    }

    pub fn batch_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
        addresses: &'a [PublicKeyBinary],
    ) -> anyhow::Result<impl Stream<Item = GatewayInfo> + 'a> {
        let entity_keys = addresses
            .iter()
            .map(|address| bs58::decode(address.to_string()).into_vec())
            .collect::<Result<Vec<_>, bs58::decode::Error>>()?;
        Ok(sqlx::query_as::<_, GatewayInfo>(&BATCH_METADATA_SQL)
            .bind(entity_keys)
            .fetch(db)
            .filter_map(|metadata| async move { metadata.ok() })
            .boxed())
    }

    use scooby::postgres::*;

    pub fn prepare_all_info_stream_query(
        device_types: &[DeviceType],
        min_refreshed_at: DateTime<Utc>,
    ) -> String {
        let mut sql = select((
            "kta.entity_key",
            "infos.location::bigint",
            "infos.device_type",
            "infos.refreshed_at",
            "infos.created_at",
        ))
        .from(
            "mobile_hotspot_infos"
                .as_("infos")
                .join("key_to_assets".as_("kta"))
                .on("infos.asset = kta.asset"),
        )
        .where_(format!(
            "(infos.refreshed_at >= to_timestamp({0}) AT TIME ZONE 'UTC' OR (infos.refreshed_at IS NULL AND infos.created_at >= to_timestamp({0}) AT TIME ZONE 'UTC'))",
            min_refreshed_at.timestamp()
        ));

        let sql = if device_types.is_empty() {
            sql
        } else {
            let device_types = device_types
                .iter()
                // The device_types field has a jsonb type but is being used as a string,
                // which forces us to add quotes.
                // .map(|v| json!(format!("{}", v)))
                .map(|v| format!("'\"{}\"'", v))
                .collect::<Vec<_>>()
                .join(", ");

            sql = sql.where_(format!("device_type::text = any(ARRAY[{0}])", device_types));
            sql
        };
        sql.to_string()
    }

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
        sql_query: &'a str,
    ) -> impl Stream<Item = GatewayInfo> + 'a {
        sqlx::query_as::<_, GatewayInfo>(sql_query)
            .fetch(db)
            .filter_map(|metadata| async move { metadata.ok() })
            .boxed()
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for GatewayInfo {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            let metadata = row
                .get::<Option<i64>, &str>("location")
                .map(|loc| GatewayMetadata {
                    location: loc as u64,
                });
            let device_type = DeviceType::from_str(
                row.get::<Json<String>, &str>("device_type")
                    .to_string()
                    .as_ref(),
            )
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
            let created_at = row.get::<DateTime<Utc>, &str>("created_at");
            // `refreshed_at` can be NULL in the database schema.
            // If so, fallback to using `created_at` as the default value of `refreshed_at`.
            let refreshed_at = row
                .get::<Option<DateTime<Utc>>, &str>("refreshed_at")
                .unwrap_or(created_at);

            Ok(Self {
                address: PublicKeyBinary::from_str(
                    &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
                )
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
                metadata,
                device_type,
                refreshed_at,
                created_at,
            })
        }
    }
}
