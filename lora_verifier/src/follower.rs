use crate::{env_var, Error, Result};

use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{self, FollowerGatewayReqV1, FollowerGatewayRespV1},
    Channel, Endpoint,
};
use helium_proto::{GatewayStakingMode, Region};
use http::Uri;
use std::ops::Not;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

type FollowerClient = follower::Client<Channel>;

#[derive(Debug, Clone)]
pub struct FollowerService {
    client: FollowerClient,
}

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

impl FollowerService {
    pub fn from_env() -> Result<Self> {
        let uri = env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?;
        Self::new(uri)
    }

    pub fn new(uri: Uri) -> Result<Self> {
        let channel = Endpoint::from(uri)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: FollowerClient::new(channel),
        })
    }

    pub async fn query_gateway_info(&mut self, address: &PublicKey) -> Result<FollowerGatewayResp> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let proto = self.client.find_gateway(req).await?.into_inner();
        let res: FollowerGatewayResp = FollowerGatewayResp::try_from(proto).unwrap();
        Ok(res)
    }
}
