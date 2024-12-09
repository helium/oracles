use chrono::{DateTime, TimeZone, Utc};
use futures::stream::BoxStream;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    GatewayMetadata as GatewayMetadataProto, GatewayMetadataV2 as GatewayMetadataProtoV2,
    WifiDeploymentInfo as WifiDeploymentInfoProto,
};
use serde::Deserialize;

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

#[derive(Clone, Debug, Deserialize)]
pub struct WifiDeploymentInfo {
    /// Antenna ID
    pub antenna: u32,
    /// The height of the hotspot above ground level in whole meters
    pub elevation: u32,
    pub azimuth: u32,
    #[serde(rename = "mechanicalDownTilt")]
    pub mechanical_down_tilt: u32,
    #[serde(rename = "electricalDownTilt")]
    pub electrical_down_tilt: u32,
}
impl From<WifiDeploymentInfoProto> for WifiDeploymentInfo {
    fn from(v: WifiDeploymentInfoProto) -> Self {
        Self {
            antenna: v.antenna,
            elevation: v.elevation,
            azimuth: v.azimuth,
            mechanical_down_tilt: v.mechanical_down_tilt,
            electrical_down_tilt: v.electrical_down_tilt,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CbrsDeploymentInfo {
    pub cbrs_radios_deployment_info: Vec<CbrsRadioDeploymentInfo>,
}

impl From<CbrsDeploymentInfoProto> for CbrsDeploymentInfo {
    fn from(v: CbrsDeploymentInfoProto) -> Self {
        Self {
            cbrs_radios_deployment_info: v
                .cbrs_radios_deployment_info
                .into_iter()
                .map(|v| v.into())
                .collect(),
        }
    }
}

impl From<CbrsRadioDeploymentInfoProto> for CbrsRadioDeploymentInfo {
    fn from(v: CbrsRadioDeploymentInfoProto) -> Self {
        Self {
            radio_id: v.radio_id,
            elevation: v.elevation,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CbrsRadioDeploymentInfo {
    /// CBSD_ID
    pub radio_id: String,
    /// The asserted elevation of the gateway above ground level in whole meters
    pub elevation: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub enum DeploymentInfo {
    #[serde(rename = "wifiInfoV0")]
    WifiDeploymentInfo(WifiDeploymentInfo),
    #[serde(rename = "cbrsInfoV0")]
    CbrsDeploymentInfo(CbrsDeploymentInfo),
}

impl From<DeploymentInfoProto> for DeploymentInfo {
    fn from(v: DeploymentInfoProto) -> Self {
        match v {
            DeploymentInfoProto::WifiDeploymentInfo(v) => {
                DeploymentInfo::WifiDeploymentInfo(v.into())
            }
            DeploymentInfoProto::CbrsDeploymentInfo(v) => {
                DeploymentInfo::CbrsDeploymentInfo(v.into())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct GatewayMetadata {
    pub location: u64,
    pub deployment_info: Option<DeploymentInfo>,
}

#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadata>,
    pub device_type: DeviceType,
    // None for V1
    pub refreshed_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
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

impl TryFrom<GatewayInfoProtoV2> for GatewayInfo {
    type Error = GatewayInfoProtoParseError;

    fn try_from(info: GatewayInfoProtoV2) -> Result<Self, Self::Error> {
        let device_type_ = info.device_type().into();

        let GatewayInfoProtoV2 {
            address,
            metadata,
            device_type: _,
            created_at,
            refreshed_at,
        } = info;

        let metadata = if let Some(metadata) = metadata {
            Some(
                u64::from_str_radix(&metadata.location, 16).map(|location| GatewayMetadata {
                    location,
                    deployment_info: metadata.deployment_info.map(|v| v.into()),
                })?,
            )
        } else {
            None
        };

        let created_at = Utc
            .timestamp_opt(created_at as i64, 0)
            .single()
            .ok_or(GatewayInfoProtoParseError::InvalidCreatedAt(created_at))?;

        let refreshed_at = Utc.timestamp_opt(refreshed_at as i64, 0).single().ok_or(
            GatewayInfoProtoParseError::InvalidRefreshedAt(info.refreshed_at),
        )?;

        Ok(Self {
            address: address.into(),
            metadata,
            device_type: device_type_,
            created_at: Some(created_at),
            refreshed_at: Some(refreshed_at),
        })
    }
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = GatewayInfoProtoParseError;

    fn try_from(info: GatewayInfoProto) -> Result<Self, Self::Error> {
        let device_type_ = info.device_type().into();

        let GatewayInfoProto {
            address,
            metadata,
            device_type: _,
        } = info;

        let metadata = if let Some(metadata) = metadata {
            Some(
                u64::from_str_radix(&metadata.location, 16).map(|location| GatewayMetadata {
                    location,
                    deployment_info: None,
                })?,
            )
        } else {
            None
        };

        Ok(Self {
            address: address.into(),
            metadata,
            device_type: device_type_,
            created_at: None,
            refreshed_at: None,
        })
    }
}

impl From<WifiDeploymentInfo> for WifiDeploymentInfoProto {
    fn from(v: WifiDeploymentInfo) -> Self {
        Self {
            antenna: v.antenna,
            elevation: v.elevation,
            azimuth: v.azimuth,
            mechanical_down_tilt: v.mechanical_down_tilt,
            electrical_down_tilt: v.electrical_down_tilt,
        }
    }
}

impl From<CbrsRadioDeploymentInfo> for CbrsRadioDeploymentInfoProto {
    fn from(v: CbrsRadioDeploymentInfo) -> Self {
        Self {
            radio_id: v.radio_id,
            elevation: v.elevation,
        }
    }
}

impl From<CbrsDeploymentInfo> for CbrsDeploymentInfoProto {
    fn from(v: CbrsDeploymentInfo) -> Self {
        Self {
            cbrs_radios_deployment_info: v
                .cbrs_radios_deployment_info
                .into_iter()
                .map(|v| v.into())
                .collect(),
        }
    }
}

impl From<DeploymentInfo> for DeploymentInfoProto {
    fn from(v: DeploymentInfo) -> Self {
        match v {
            DeploymentInfo::WifiDeploymentInfo(v) => {
                DeploymentInfoProto::WifiDeploymentInfo(v.into())
            }
            DeploymentInfo::CbrsDeploymentInfo(v) => {
                DeploymentInfoProto::CbrsDeploymentInfo(v.into())
            }
        }
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

#[derive(thiserror::Error, Debug)]
pub enum GatewayInfoToProtoError {
    #[error("Invalid location: {0}")]
    InvalidLocation(#[from] hextree::Error),
    #[error("created_at is None")]
    CreatedAtIsNone,
    #[error("refreshed_at is None")]
    RefreshedAtIsNone,
}

impl TryFrom<GatewayInfo> for GatewayInfoProtoV2 {
    type Error = GatewayInfoToProtoError;

    fn try_from(info: GatewayInfo) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            let deployment_info = metadata.deployment_info.map(|v| v.into());
            Some(GatewayMetadataProtoV2 {
                location: hextree::Cell::from_raw(metadata.location)?.to_string(),
                deployment_info,
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type: info.device_type as i32,
            created_at: info
                .created_at
                .ok_or(GatewayInfoToProtoError::CreatedAtIsNone)?
                .timestamp() as u64,
            refreshed_at: info
                .refreshed_at
                .ok_or(GatewayInfoToProtoError::RefreshedAtIsNone)?
                .timestamp() as u64,
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
    use crate::gateway_info::DeploymentInfo;
    use chrono::{DateTime, Utc};
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{types::Json, PgExecutor, Row};
    use std::{collections::HashSet, str::FromStr};

    const GET_METADATA_SQL: &str = r#"
            select kta.entity_key, infos.location::bigint, infos.device_type,
                infos.refreshed_at, infos.created_at, infos.deployment_info
            from mobile_hotspot_infos infos
            join key_to_assets kta on infos.asset = kta.asset
        "#;
    const BATCH_SQL_WHERE_SNIPPET: &str = " where kta.entity_key = any($1::bytea[]) ";
    const DEVICE_TYPES_WHERE_SNIPPET: &str = " where device_type::text = any($1) ";

    const GET_UPDATED_RADIOS: &str =
        "SELECT entity_key FROM mobile_radio_tracker WHERE last_changed_at >= $1";

    lazy_static::lazy_static! {
        static ref BATCH_METADATA_SQL: String = format!("{GET_METADATA_SQL} {BATCH_SQL_WHERE_SNIPPET}");
        static ref DEVICE_TYPES_METADATA_SQL: String = format!("{GET_METADATA_SQL} {DEVICE_TYPES_WHERE_SNIPPET}");
    }

    pub async fn get_updated_radios(
        db: impl PgExecutor<'_>,
        min_updated_at: DateTime<Utc>,
    ) -> anyhow::Result<HashSet<PublicKeyBinary>> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar(GET_UPDATED_RADIOS)
            .bind(min_updated_at)
            .fetch_all(db)
            .await?;
        let mut radios = HashSet::new();

        for row in rows {
            let entity_key_b: &[u8] = &row;
            let entity_key = bs58::encode(entity_key_b).into_string();
            let pk = PublicKeyBinary::from_str(&entity_key)?;
            radios.insert(pk);
        }

        Ok(radios)
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
        device_types: &'a [DeviceType],
    ) -> impl Stream<Item = GatewayInfo> + 'a {
        match device_types.is_empty() {
            true => sqlx::query_as::<_, GatewayInfo>(GET_METADATA_SQL)
                .fetch(db)
                .filter_map(|metadata| async move { metadata.ok() })
                .boxed(),
            false => sqlx::query_as::<_, GatewayInfo>(&DEVICE_TYPES_METADATA_SQL)
                .bind(
                    device_types
                        .iter()
                        // The device_types field has a jsonb type but is being used as a string,
                        // which forces us to add quotes.
                        .map(|v| format!("\"{}\"", v))
                        .collect::<Vec<_>>(),
                )
                .fetch(db)
                .filter_map(|metadata| async move { metadata.ok() })
                .boxed(),
        }
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for GatewayInfo {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            let deployment_info =
                match row.try_get::<Option<Json<DeploymentInfo>>, &str>("deployment_info") {
                    Ok(di) => di.map(|v| v.0),
                    // We shouldn't fail if an error occurs in this case.
                    // This is because the data in this column could be inconsistent,
                    // and we don't want to break backward compatibility.
                    Err(_e) => None,
                };

            // If location field is None, GatewayMetadata also is None, even if deployment_info is present.
            // Because "location" is mandatory field
            let metadata = row
                .get::<Option<i64>, &str>("location")
                .map(|loc| GatewayMetadata {
                    location: loc as u64,
                    deployment_info,
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
                refreshed_at: Some(refreshed_at),
                created_at: Some(created_at),
            })
        }
    }
}
