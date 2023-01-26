use crate::txn_service::{TransactionClient, TransactionClients};
use helium_proto::services::{follower, transaction, Channel, Endpoint};
use serde::Deserialize;
use std::time::Duration;

// Wrapper to make serde happy with an Option<Vec<http::Uri>>
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct NodeUrl {
    #[serde(with = "http_serde::uri")]
    pub url: http::Uri,
}

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
    /// Optional list of Node urls to submit txns to
    pub submission_urls: Option<Vec<NodeUrl>>,
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
    pub fn connect_follower(&self) -> follower::Client<Channel> {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy();
        follower::Client::new(channel)
    }

    pub fn connect_transactions(&self) -> TransactionClient {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy();
        transaction::Client::new(channel)
    }

    pub fn connect_org(&self) -> OrgClient<Channel> {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy();
        OrgClient::new(channel)
    }

    pub fn connect_multiple_transactions(&self) -> Option<TransactionClients> {
        match &self.submission_urls {
            Some(urls) => {
                let clients = urls
                    .iter()
                    .map(|node_url| {
                        let channel = Endpoint::from(node_url.url.clone())
                            .connect_timeout(Duration::from_secs(self.connect))
                            .timeout(Duration::from_secs(self.rpc))
                            .connect_lazy();
                        transaction::Client::new(channel)
                    })
                    .collect();
                Some(clients)
            }
            None => None,
        }
    }
}
