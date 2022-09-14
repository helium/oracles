// TODO: Remove
#![allow(dead_code, unused)]

// TODO: This should really be in something like poc-common
// OR we'll use the follower service from poc-verifier once that lands

use crate::{env_var, Result};
use helium_proto::services::{follower, Channel, Endpoint};
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
}
