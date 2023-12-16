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
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = std::num::ParseIntError;

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
        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type,
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
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum DeviceType {
    Cbrs,
    WifiIndoor,
    WifiOutdoor,
}

impl From<DeviceTypeProto> for DeviceType {
    fn from(dtp: DeviceTypeProto) -> Self {
        match dtp {
            DeviceTypeProto::Cbrs => DeviceType::Cbrs,
            DeviceTypeProto::WifiIndoor => DeviceType::WifiIndoor,
            DeviceTypeProto::WifiOutdoor => DeviceType::WifiOutdoor,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid device type string")]
pub struct DeviceTypeParseError;

impl std::str::FromStr for DeviceType {
    type Err = DeviceTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "cbrs" => Self::Cbrs,
            "wifiIndoor" => Self::WifiIndoor,
            "wifiOutdoor" => Self::WifiOutdoor,
            _ => return Err(DeviceTypeParseError),
        };
        Ok(result)
    }
}

pub(crate) mod db {
    use super::{DeviceType, GatewayInfo, GatewayMetadata};
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{types::Json, PgExecutor, Row};
    use std::str::FromStr;

    const GET_METADATA_SQL: &str = r#"
            select kta.entity_key, infos.location::bigint, infos.device_type
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

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = GatewayInfo> + 'a {
        sqlx::query_as::<_, GatewayInfo>(GET_METADATA_SQL)
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
            Ok(Self {
                address: PublicKeyBinary::from_str(
                    &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
                )
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
                metadata,
                device_type,
            })
        }
    }
}
