use crate::{Error, Result};
use async_trait::async_trait;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::follower::GatewayInfo as GatewayInfoProto, GatewayStakingMode, Region,
};

#[async_trait]
pub trait GatewayInfoResolver {
    async fn resolve_gateway_info(&mut self, address: &PublicKeyBinary) -> Result<GatewayInfo>;
}

#[derive(Debug, Clone)]
pub struct GatewayInfo {
    pub location: Option<u64>,
    pub address: Vec<u8>,
    pub owner: Vec<u8>,
    pub staking_mode: GatewayStakingMode,
    pub gain: i32,
    pub region: Region,
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = Error;
    fn try_from(v: GatewayInfoProto) -> Result<Self> {
        let staking_mode: GatewayStakingMode = GatewayStakingMode::from_i32(v.staking_mode)
            .ok_or_else(|| Error::StakingMode(format!("{:?}", v.staking_mode)))?;
        let region: Region =
            Region::from_i32(v.region).ok_or_else(|| Error::Region(format!("{:?}", v.region)))?;
        let location = u64::from_str_radix(&v.location, 16).ok();
        Ok(Self {
            location,
            address: v.address,
            owner: v.owner,
            staking_mode,
            gain: v.gain,
            region,
        })
    }
}

impl TryFrom<GatewayInfo> for GatewayInfoProto {
    type Error = Error;
    fn try_from(v: GatewayInfo) -> Result<Self> {
        let location = match v.location {
            None => String::new(),
            Some(loc) => loc.to_string(),
        };
        Ok(Self {
            location,
            address: v.address,
            owner: v.owner,
            staking_mode: v.staking_mode as i32,
            gain: v.gain,
            region: v.region as i32,
            region_params: None,
        })
    }
}
