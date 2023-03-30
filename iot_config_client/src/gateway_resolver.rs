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
    pub location: Option<u64>,
    pub elevation: Option<i32>,
    pub gain: i32,
    pub is_full_hotspot: bool,
    pub region: Region,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayResolverError {
    #[error("unsupported region {0}")]
    Region(String),
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = GatewayResolverError;
    fn try_from(v: GatewayInfoProto) -> Result<Self, GatewayResolverError> {
        let region: Region = Region::from_i32(v.region)
            .ok_or_else(|| GatewayResolverError::Region(format!("{:?}", v.region)))?;
        let location = u64::from_str_radix(&v.location, 16).ok();
        let elevation = match location {
            Some(_) => Some(v.elevation),
            None => None,
        };
        Ok(Self {
            address: v.address.into(),
            location,
            elevation,
            gain: v.gain,
            is_full_hotspot: v.is_full_hotspot,
            region,
        })
    }
}
