use crate::iot_config_client::IotConfigClientError;
use async_trait::async_trait;
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::iot_config::GatewayInfo as GatewayInfoProto, Region};

#[async_trait]
pub trait GatewayInfoResolver {
    async fn resolve_gateway_info(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, IotConfigClientError>;
}

#[derive(Debug, Clone)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub is_full_hotspot: bool,
    pub location: u64,
    pub elevation: i32,
    pub gain: i32,
    pub region: Region,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayResolverError {
    #[error("unsupported region {0}")]
    Region(String),
    #[error("gateway not asserted: {0}")]
    GatewayNotAsserted(PublicKeyBinary),
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = GatewayResolverError;
    fn try_from(v: GatewayInfoProto) -> Result<Self, GatewayResolverError> {
        let location = u64::from_str_radix(&v.location, 16).ok();
        //TODO: use the updated proto when available
        //      which will have a metadata field defined as an Option
        //      if Some(metadata) then we have an asserted gateway
        //      if None the gateway is unasserted
        //      we only return Ok(GatewayInfo) for asserted gateways
        match location {
            Some(_) => Ok(Self {
                address: v.address.into(),
                is_full_hotspot: v.is_full_hotspot,
                location: location.unwrap(),
                elevation: v.elevation,
                gain: v.gain,
                region: Region::from_i32(v.region).unwrap(),
            }),
            None => Err(GatewayResolverError::GatewayNotAsserted(v.address.into())),
        }
    }
}
