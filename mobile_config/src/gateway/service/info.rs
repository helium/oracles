use crate::gateway::{
    self,
    db::{Gateway, GatewayType},
};
use chrono::{DateTime, TimeZone, Utc};
use futures::{stream::BoxStream, Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    GatewayMetadata as GatewayMetadataProto, GatewayMetadataV2 as GatewayMetadataProtoV2,
    WifiDeploymentInfo as WifiDeploymentInfoProto,
};
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;
use strum::IntoEnumIterator;

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CbrsRadioDeploymentInfo {
    /// CBSD_ID
    pub radio_id: String,
    /// The asserted elevation of the gateway above ground level in whole meters
    pub elevation: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DeploymentInfo {
    #[serde(rename = "wifiInfoV0")]
    WifiDeploymentInfo(WifiDeploymentInfo),
    #[serde(rename = "cbrsInfoV0")]
    CbrsDeploymentInfo(CbrsDeploymentInfo),
}

impl DeploymentInfo {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
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
    // Optional fields are None for GatewayInfoProto (V1)
    pub created_at: Option<DateTime<Utc>>,
    // updated_at refers to the last time the data was actually changed.
    pub updated_at: Option<DateTime<Utc>>,
    // refreshed_at indicates the last time the chain was consulted, regardless of data changes.
    pub refreshed_at: Option<DateTime<Utc>>,
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
    #[error("Invalid updated_at: {0}")]
    InvalidUpdatedAt(u64),
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
            updated_at,
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

        let updated_at = Utc
            .timestamp_opt(updated_at as i64, 0)
            .single()
            .ok_or(GatewayInfoProtoParseError::InvalidUpdatedAt(updated_at))?;

        Ok(Self {
            address: address.into(),
            metadata,
            device_type: device_type_,
            created_at: Some(created_at),
            updated_at: Some(updated_at),
            refreshed_at: None,
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
            updated_at: None,
            refreshed_at: None,
        })
    }
}

impl From<Gateway> for GatewayInfo {
    fn from(gateway: Gateway) -> Self {
        let metadata = if let Some(location) = gateway.location {
            Some(GatewayMetadata {
                location,
                deployment_info: Some(gateway::service::info::DeploymentInfo::WifiDeploymentInfo(
                    WifiDeploymentInfo {
                        antenna: gateway.antenna.unwrap_or(0),
                        elevation: gateway.elevation.unwrap_or(0),
                        azimuth: gateway.azimuth.unwrap_or(0),
                        mechanical_down_tilt: 0,
                        electrical_down_tilt: 0,
                    },
                )),
            })
        } else {
            None
        };

        Self {
            address: gateway.address,
            metadata,
            device_type: gateway.gateway_type.into(),
            created_at: Some(gateway.created_at),
            updated_at: Some(gateway.updated_at),
            refreshed_at: Some(gateway.refreshed_at),
        }
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
    #[error("updated_at is None")]
    UpdatedAtIsNone,
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
            updated_at: info
                .updated_at
                .ok_or(GatewayInfoToProtoError::UpdatedAtIsNone)?
                .timestamp() as u64,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
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

impl From<GatewayType> for DeviceType {
    fn from(gt: GatewayType) -> Self {
        match gt {
            GatewayType::WifiIndoor => DeviceType::WifiIndoor,
            GatewayType::WifiOutdoor => DeviceType::WifiOutdoor,
            GatewayType::WifiDataOnly => DeviceType::WifiDataOnly,
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

pub async fn get_by_address(
    db: impl PgExecutor<'_>,
    pubkey_bin: &PublicKeyBinary,
) -> anyhow::Result<Option<GatewayInfo>> {
    let gateway = Gateway::get_by_address(db, pubkey_bin).await?;
    Ok(gateway.map(|g| g.into()))
}

pub fn stream_by_addresses<'a>(
    db: impl PgExecutor<'a> + 'a,
    addresses: &'a [PublicKeyBinary],
    min_updated_at: DateTime<Utc>,
) -> anyhow::Result<impl Stream<Item = GatewayInfo> + 'a> {
    Ok(Gateway::stream_by_addresses(db, addresses, min_updated_at).map(|gateway| gateway.into()))
}

pub fn stream_by_types<'a>(
    db: impl PgExecutor<'a> + 'a,
    types: &'a [DeviceType],
    min_date: DateTime<Utc>,
) -> anyhow::Result<impl Stream<Item = GatewayInfo> + 'a> {
    let gateway_types = if types.is_empty() {
        GatewayType::iter().collect()
    } else {
        types
            .iter()
            .filter_map(|t| t.clone().try_into().ok())
            .collect::<Vec<GatewayType>>()
    };

    Ok(Gateway::stream_by_types(db, gateway_types, min_date, None).map(|gateway| gateway.into()))
}
