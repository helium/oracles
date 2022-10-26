use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Scrape endpoint for metrics
    #[serde(default = "default_metrics_endpoint")]
    pub endpoint: String,
}

pub fn default_metrics_endpoint() -> String {
    "127.0.0.1:19000".to_string()
}
