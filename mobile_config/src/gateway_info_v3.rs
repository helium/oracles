use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    // gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    // CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    // CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    // GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    // GatewayMetadata as GatewayMetadataProto, GatewayMetadataV2 as GatewayMetadataProtoV2,
    // WifiDeploymentInfo as WifiDeploymentInfoProto,
    GatewayInfoV3 as GatewayInfoProtoV3,
};
use sqlx::PgExecutor;

use futures::stream::Stream;

#[derive(Clone, Debug)]
pub struct GatewayInfoV3 {
    pub address: PublicKeyBinary,
    // pub metadata: Option<GatewayMetadata>,
    // pub device_type: DeviceType,
    // Optional fields are None for GatewayInfoProto (V1)
    pub created_at: DateTime<Utc>,
    // updated_at refers to the last time the data was actually changed.
    pub updated_at: DateTime<Utc>,
    // refreshed_at indicates the last time the chain was consulted, regardless of data changes.
    pub refreshed_at: DateTime<Utc>,
}

impl TryFrom<GatewayInfoV3> for GatewayInfoProtoV3 {
    type Error = hextree::Error;

    fn try_from(_info: GatewayInfoV3) -> Result<Self, Self::Error> {
        todo!()
        // let metadata = if let Some(metadata) = info.metadata {
        //     Some(GatewayMetadataProto {
        //         location: hextree::Cell::from_raw(metadata.location)?.to_string(),
        //     })
        // } else {
        //     None
        // };
        // Ok(Self {
        //     address: info.address.into(),
        //     metadata,
        //     device_type: info.device_type as i32,
        // })
    }
}

pub fn all_info_stream_v3<'a>(
    _db: impl PgExecutor<'a> + 'a,
    // device_types: &'a [DeviceType],
) -> impl Stream<Item = GatewayInfoV3> + 'a {
    // TODO
    futures::stream::empty::<GatewayInfoV3>()
}
