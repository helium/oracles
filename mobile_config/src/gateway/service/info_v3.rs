use crate::gateway::{
    db::{Gateway, GatewayType},
    service::info::DeviceTypeParseError,
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    DeploymentInfo as DeploymentInfoProto, DeviceTypeV2 as DeviceTypeProtoV2,
    GatewayInfoV3 as GatewayInfoProtoV3, GatewayMetadataV3 as GatewayMetadataProtoV3,
    LocationInfo as LocationInfoProto,
};
use sqlx::PgExecutor;

#[derive(Clone, Debug)]
pub struct LocationInfo {
    pub location: u64,
    pub location_changed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct GatewayMetadataV3 {
    pub location_info: LocationInfo,
    pub deployment_info: Option<DeploymentInfoProto>,
}

#[derive(Clone, Debug)]
pub enum DeviceTypeV2 {
    Indoor,
    Outdoor,
    DataOnly,
}

impl std::str::FromStr for DeviceTypeV2 {
    type Err = DeviceTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "wifiIndoor" => Self::Indoor,
            "wifiOutdoor" => Self::Outdoor,
            "wifiDataOnly" => Self::DataOnly,
            _ => return Err(DeviceTypeParseError),
        };
        Ok(result)
    }
}

impl From<GatewayType> for DeviceTypeV2 {
    fn from(gt: GatewayType) -> Self {
        match gt {
            GatewayType::WifiIndoor => DeviceTypeV2::Indoor,
            GatewayType::WifiOutdoor => DeviceTypeV2::Outdoor,
            GatewayType::WifiDataOnly => DeviceTypeV2::DataOnly,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GatewayInfoV3 {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadataV3>,
    pub device_type: DeviceTypeV2,
    pub created_at: DateTime<Utc>,
    // updated_at refers to the last time the data was actually changed.
    pub updated_at: DateTime<Utc>,
    // refreshed_at indicates the last time the chain was consulted, regardless of data changes.
    pub refreshed_at: DateTime<Utc>,
    pub num_location_asserts: i32,
}

impl From<Gateway> for GatewayInfoV3 {
    fn from(gateway: Gateway) -> Self {
        let metadata = if let Some(location) = gateway.location {
            let location_info = LocationInfo {
                location,
                location_changed_at: gateway.location_changed_at.unwrap_or(gateway.created_at),
            };
            Some(GatewayMetadataV3 {
                location_info,
                deployment_info: Some(DeploymentInfoProto {
                    antenna: gateway.antenna.unwrap_or(0),
                    elevation: gateway.elevation.unwrap_or(0),
                    azimuth: gateway.azimuth.unwrap_or(0),
                }),
            })
        } else {
            None
        };

        Self {
            address: gateway.address,
            metadata,
            device_type: gateway.gateway_type.into(),
            created_at: gateway.created_at,
            updated_at: gateway.last_changed_at,
            refreshed_at: gateway.refreshed_at,
            num_location_asserts: gateway.location_asserts.unwrap_or(0) as i32,
        }
    }
}

impl From<DeviceTypeProtoV2> for DeviceTypeV2 {
    fn from(value: DeviceTypeProtoV2) -> Self {
        match value {
            DeviceTypeProtoV2::Indoor => DeviceTypeV2::Indoor,
            DeviceTypeProtoV2::Outdoor => DeviceTypeV2::Outdoor,
            DeviceTypeProtoV2::DataOnly => DeviceTypeV2::DataOnly,
        }
    }
}

impl TryFrom<GatewayInfoV3> for GatewayInfoProtoV3 {
    type Error = hextree::Error;

    fn try_from(info: GatewayInfoV3) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            let location_info = LocationInfoProto {
                location: hextree::Cell::from_raw(metadata.location_info.location)?.to_string(),
                location_changed_at: metadata.location_info.location_changed_at.timestamp() as u64,
            };
            let deployment_info = metadata.deployment_info.map(|di| DeploymentInfoProto {
                antenna: di.antenna,
                elevation: di.elevation,
                azimuth: di.azimuth,
            });

            Some(GatewayMetadataProtoV3 {
                location_info: Some(location_info),
                deployment_info,
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type: info.device_type as i32,
            created_at: info.created_at.timestamp() as u64,
            updated_at: info.updated_at.timestamp() as u64,
            num_location_asserts: info.num_location_asserts as u64,
        })
    }
}

pub fn stream_by_types<'a>(
    db: impl PgExecutor<'a> + 'a,
    types: &'a [DeviceTypeV2],
    min_date: DateTime<Utc>,
    min_location_changed_at: Option<DateTime<Utc>>,
) -> anyhow::Result<impl Stream<Item = GatewayInfoV3> + 'a> {
    let gateway_types = if types.is_empty() {
        vec![
            GatewayType::WifiIndoor,
            GatewayType::WifiOutdoor,
            GatewayType::WifiDataOnly,
        ]
    } else {
        types
            .iter()
            .map(|t| t.clone().into())
            .collect::<Vec<GatewayType>>()
    };

    Ok(
        Gateway::stream_by_types(db, gateway_types, min_date, min_location_changed_at)
            .map(|gateway| gateway.into()),
    )
}
