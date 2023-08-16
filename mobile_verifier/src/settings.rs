use chrono::{DateTime, TimeZone, Utc};
use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Reward period in hours. (Default is 24)
    #[serde(default = "default_reward_period")]
    pub rewards: i64,
    #[serde(default = "default_reward_offset_minutes")]
    pub reward_offset_minutes: i64,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub data_transfer_ingest: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub price_tracker: price::price_tracker::Settings,
    pub config_client: mobile_config::ClientSettings,
    #[serde(default = "default_start_after")]
    pub start_after: u64,
    #[serde(default = "default_max_heartbeat_distance_from_coverage_km")]
    pub max_heartbeat_distance_from_coverage_km: f64,
    #[serde(default = "default_disable_discovery_loc_rewards_to_s3")]
    pub disable_discovery_loc_rewards_to_s3: bool,
    pub modeled_coverage_start_timestamp: DateTime<Utc>,
}

pub fn default_disable_discovery_loc_rewards_to_s3() -> bool {
    true
}

pub fn default_log() -> String {
    "mobile_verifier=debug,poc_store=info".to_string()
}

pub fn default_start_after() -> u64 {
    0
}

pub fn default_reward_period() -> i64 {
    24
}

pub fn default_reward_offset_minutes() -> i64 {
    30
}

pub fn default_max_heartbeat_distance_from_coverage_km() -> f64 {
    2.5
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of VERIFY)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("VERIFY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn start_after(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.start_after as i64, 0)
            .single()
            .unwrap()
    }
}
