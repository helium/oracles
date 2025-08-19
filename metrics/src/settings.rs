use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    /// Scrape endpoint for metrics
    #[serde(default = "default_metrics_endpoint")]
    pub endpoint: SocketAddr,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            endpoint: default_metrics_endpoint(),
        }
    }
}

fn default_metrics_endpoint() -> SocketAddr {
    "127.0.0.1:19000".parse().unwrap()
}
