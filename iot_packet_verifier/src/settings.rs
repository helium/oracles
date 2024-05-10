use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "iot_packet_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Data credit burn period in minutes. Default is 1.
    #[serde(with = "humantime_serde", default = "default_burn_period")]
    pub burn_period: Duration,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub iot_config_client: iot_config::client::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    #[serde(default)]
    pub enable_solana_integration: bool,
    /// Minimum data credit balance required for a payer before we disable them
    #[serde(default = "default_minimum_allowed_balance")]
    pub minimum_allowed_balance: u64,
    pub solana: Option<solana::burn::Settings>,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    /// Number of minutes we should sleep before checking to re-enable
    /// any disabled orgs.
    #[serde(default = "default_monitor_funds_period")]
    pub monitor_funds_period: Duration,
}

pub fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

pub fn default_burn_period() -> Duration {
    humantime::parse_duration("1 minute").unwrap()
}

pub fn default_log() -> String {
    "iot_packet_verifier=debug".to_string()
}

pub fn default_minimum_allowed_balance() -> u64 {
    3_500_000
}

pub fn default_monitor_funds_period() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
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
            .add_source(Environment::with_prefix("PACKET_VERIFY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
