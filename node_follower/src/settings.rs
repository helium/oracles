use crate::Result;
use helium_proto::services::{follower, transaction, Channel, Endpoint};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Local grpc url to node follower for rewards tracking and submission
    #[serde(with = "http_serde::uri", default = "default_url")]
    pub url: http::Uri,
    /// Start block to start streaming followed transactions from.
    pub block: Option<i64>,
    /// Connect timeout for follower in seconds. Default 5
    #[serde(default = "default_connect_timeout")]
    pub connect: u64,
    /// RPC timeout for follower in seconds. Default 5
    #[serde(default = "default_rpc_timeout")]
    pub rpc: u64,
    /// batch size for gateway stream results. Default 100
    #[serde(default = "default_batch_size")]
    pub batch: u32,
}

pub fn default_url() -> http::Uri {
    http::Uri::from_static("http://127.0.0.1:8080")
}

pub fn default_connect_timeout() -> u64 {
    5
}

pub fn default_rpc_timeout() -> u64 {
    5
}

pub fn default_batch_size() -> u32 {
    100
}

impl Settings {
    pub fn connect_follower(&self) -> Result<follower::Client<Channel>> {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy();
        Ok(follower::Client::new(channel))
    }

    pub fn connect_transactions(&self) -> Result<transaction::Client<Channel>> {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy();
        Ok(transaction::Client::new(channel))
    }
}
