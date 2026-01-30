use crate::gateway::{
    db::{Gateway, GatewayType},
    service::info_v3::{DeviceTypeV2, GatewayMetadataV3, LocationInfo},
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    DeploymentInfo as DeploymentInfoProto, GatewayInfoV4 as GatewayInfoProtoV4,
    GatewayMetadataV3 as GatewayMetadataProtoV3, LocationInfo as LocationInfoProto,
};
use sqlx::PgExecutor;
use strum::IntoEnumIterator;

#[derive(Clone, Debug)]
pub struct GatewayInfoV4 {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadataV3>,
    pub device_type: DeviceTypeV2,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub refreshed_at: DateTime<Utc>,
    pub num_location_asserts: i32,
    pub owner: Option<String>,
    pub owner_changed_at: Option<DateTime<Utc>>,
}

impl TryFrom<Gateway> for GatewayInfoV4 {
    type Error = hextree::Error;
    fn try_from(gateway: Gateway) -> Result<Self, Self::Error> {
        let metadata = if let Some(location) = gateway.location {
            let location_info = LocationInfo {
                location: hextree::Cell::from_raw(location)?,
                location_changed_at: gateway.location_changed_at.unwrap_or(gateway.created_at),
            };
            let deployment_info = match (gateway.antenna, gateway.elevation, gateway.azimuth) {
                (None, None, None) => None,
                _ => Some(DeploymentInfoProto {
                    antenna: gateway.antenna.unwrap_or(0),
                    elevation: gateway.elevation.unwrap_or(0),
                    azimuth: gateway.azimuth.unwrap_or(0),
                }),
            };
            Some(GatewayMetadataV3 {
                location_info,
                deployment_info,
            })
        } else {
            None
        };

        Ok(Self {
            address: gateway.address,
            metadata,
            device_type: gateway.gateway_type.into(),
            created_at: gateway.created_at,
            updated_at: gateway.last_changed_at,
            refreshed_at: gateway.refreshed_at,
            num_location_asserts: gateway.location_asserts.unwrap_or(0) as i32,
            owner: gateway.owner,
            owner_changed_at: gateway.owner_changed_at,
        })
    }
}

impl TryFrom<GatewayInfoV4> for GatewayInfoProtoV4 {
    type Error = hextree::Error;

    fn try_from(info: GatewayInfoV4) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            let location_info = LocationInfoProto {
                location: metadata.location_info.location.to_string(),
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
            owner: info.owner.unwrap_or_default(),
            owner_changed_at: info
                .owner_changed_at
                .map(|t| t.timestamp() as u64)
                .unwrap_or(0),
        })
    }
}

pub fn stream_by_types<'a>(
    db: impl PgExecutor<'a> + 'a,
    types: &'a [DeviceTypeV2],
    min_date: DateTime<Utc>,
    min_location_changed_at: Option<DateTime<Utc>>,
    min_owner_changed_at: Option<DateTime<Utc>>,
) -> impl Stream<Item = GatewayInfoV4> + 'a {
    let gateway_types = if types.is_empty() {
        GatewayType::iter().collect::<Vec<_>>()
    } else {
        types
            .iter()
            .map(|t| t.clone().into())
            .collect::<Vec<GatewayType>>()
    };

    Gateway::stream_by_types_v4(
        db,
        gateway_types,
        min_date,
        min_location_changed_at,
        min_owner_changed_at,
    )
    .filter_map(|gateway| async move { gateway.try_into().ok() })
}
