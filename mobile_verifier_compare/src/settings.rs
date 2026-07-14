use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

/// Minimal settings for the compare-trino CLI: just a Postgres pool and a
/// Trino client. Loaded independently of `crate::Settings` so the heavier
/// production fields (buckets, signing keypair, price_tracker, …) don't have
/// to be filled in for a one-off diff run.
#[derive(Debug, Deserialize)]
pub struct Settings {
    #[serde(default = "default_log")]
    pub log: String,
    pub database: db_store::Settings,
    pub trino: trino_client::Settings,
}

fn default_log() -> String {
    "mobile_verifier=info".to_string()
}

impl Settings {
    /// Same loader semantics as `crate::Settings::new`: optional TOML file +
    /// environment overrides under the `MV__` prefix.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();
        if let Some(file) = path {
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        builder
            .add_source(
                Environment::with_prefix("MV")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|c| c.try_deserialize())
    }
}
