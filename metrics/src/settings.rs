use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Scrape endpoint for metrics
    #[serde(default = "default_metrics_endpoint")]
    pub endpoint: SocketAddr,
}

fn default_metrics_endpoint() -> SocketAddr {
    "127.0.0.1:19000".parse().unwrap()
}
