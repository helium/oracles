use crate::{env_var, Result};

use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{self, FollowerGatewayReqV1, FollowerGatewayRespV1},
    Channel, Endpoint,
};
use http::Uri;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

type FollowerClient = follower::Client<Channel>;

#[derive(Debug, Clone)]
pub struct FollowerService {
    client: FollowerClient,
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

    pub async fn query_gateway_info(
        &mut self,
        address: &PublicKey,
    ) -> Result<FollowerGatewayRespV1> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.client.find_gateway(req).await?.into_inner();
        Ok(res)
    }
}
