use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Burn period in hours. (Default is 1 hour)
    #[serde(with = "humantime_serde", default = "default_burn_period")]
    pub burn_period: Duration,
    /// Minimum burn period when error. (Default is 15 minutes)
    #[serde(with = "humantime_serde", default = "default_min_burn_period")]
    pub min_burn_period: Duration,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    #[serde(default)]
    pub enable_solana_integration: bool,
    pub solana: Option<solana::burn::Settings>,
    pub config_client: mobile_config::ClientSettings,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    #[serde(with = "humantime_serde", default = "default_purger_interval")]
    pub purger_interval: Duration,
    #[serde(with = "humantime_serde", default = "default_purger_max_age")]
    pub purger_max_age: Duration,
    // default delay between retry attempts at submitting solana txns, in seconds
    #[serde(default = "default_txn_retry_delay")]
    pub txn_retry_delay: Duration,
}

pub fn default_txn_retry_delay() -> Duration {
    humantime::parse_duration("1 seconds").unwrap()
}

fn default_purger_interval() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_purger_max_age() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_log() -> String {
    "mobile_packet_verifier=debug,poc_store=info".to_string()
}

fn default_burn_period() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_min_burn_period() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new(path: Option<impl AsRef<Path>>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of VERIFY)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("MOBILE_PACKET_VERIFY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
