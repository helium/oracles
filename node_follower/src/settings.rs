use helium_proto::services::{follower, transaction, Channel, Endpoint};
use serde::Deserialize;
use std::time::Duration;

pub type FollowerClients = Vec<follower::Client<Channel>>;
pub type TxnClients = Vec<transaction::Client<Channel>>;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct NodeUrl {
    #[serde(with = "http_serde::uri")]
    pub url: http::Uri,
}

impl NodeUrl {
    fn new(url: http::Uri) -> Self {
        Self { url }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Local grpc urls to node followers for rewards tracking and submission
    #[serde(default = "default_urls")]
    pub urls: Vec<NodeUrl>,
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

pub fn default_urls() -> Vec<NodeUrl> {
    let http_url = http::Uri::from_static("http://127.0.0.1:8080");
    vec![NodeUrl::new(http_url)]
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
    pub fn connect_followers(&self) -> FollowerClients {
        self.urls
            .iter()
            .map(|node_url| {
                let channel = Endpoint::from(node_url.url.clone())
                    .connect_timeout(Duration::from_secs(self.connect))
                    .timeout(Duration::from_secs(self.rpc))
                    .connect_lazy();
                follower::Client::new(channel)
            })
            .collect()
    }

    pub fn connect_transactions(&self) -> TxnClients {
        self.urls
            .iter()
            .map(|node_url| {
                let channel = Endpoint::from(node_url.url.clone())
                    .connect_timeout(Duration::from_secs(self.connect))
                    .timeout(Duration::from_secs(self.rpc))
                    .connect_lazy();
                transaction::Client::new(channel)
            })
            .collect()
    }
}
