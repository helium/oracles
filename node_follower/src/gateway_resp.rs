use crate::{Error, Result};
use helium_proto::{services::follower::FollowerGatewayRespV1, GatewayStakingMode, Region};
use std::ops::Not;

#[derive(Debug, Clone)]
pub struct FollowerGatewayResp {
    pub height: u64,
    pub location: Option<u64>,
    pub address: Vec<u8>,
    pub owner: Vec<u8>,
    pub staking_mode: GatewayStakingMode,
    pub gain: i32,
    pub region: Region,
}

impl TryFrom<FollowerGatewayRespV1> for FollowerGatewayResp {
    type Error = Error;
    fn try_from(v: FollowerGatewayRespV1) -> Result<Self> {
        let staking_mode: GatewayStakingMode = GatewayStakingMode::from_i32(v.staking_mode)
            .ok_or_else(|| Error::custom("unsupported staking_mode"))?;
        let region: Region =
            Region::from_i32(v.region).ok_or_else(|| Error::custom("unsupported region"))?;
        let location = v
            .location
            .is_empty()
            .not()
            .then(|| v.location.parse::<u64>().unwrap());
        Ok(Self {
            height: v.height,
            location,
            address: v.address,
            owner: v.owner,
            staking_mode,
            gain: v.gain,
            region,
        })
    }
}

impl TryFrom<FollowerGatewayResp> for FollowerGatewayRespV1 {
    type Error = Error;
    fn try_from(v: FollowerGatewayResp) -> Result<Self> {
        let location = match v.location {
            None => String::new(),
            Some(loc) => loc.to_string(),
        };
        Ok(Self {
            height: v.height,
            location,
            address: v.address,
            owner: v.owner,
            staking_mode: v.staking_mode as i32,
            gain: v.gain,
            region: v.region as i32,
        })
    }
}
