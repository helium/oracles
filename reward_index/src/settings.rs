use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{fmt, path::Path, time::Duration};

/// Mode to start the indexer in. Each mode uses different files from
/// the verifier
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Iot,
    Mobile,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iot => f.write_str("iot"),
            Self::Mobile => f.write_str("mobile"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "poc_entropy=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Check interval in seconds. (Default is 900; 15 minutes)
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    /// Mode to run the server in (iot or mobile). Required
    pub mode: Mode,
    /// Required when running in mode=iot
    pub operation_fund_key: Option<String>,
    #[serde(default = "default_unallocated_reward_entity_key")]
    pub unallocated_reward_entity_key: String,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,

    pub database: db_store::Settings,
    #[serde(default)]
    pub file_store: file_store::Settings,
    pub input_bucket: String,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
}

fn default_interval() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_log() -> String {
    "reward_index=debug,poc_store=info".to_string()
}

fn default_unallocated_reward_entity_key() -> String {
    "not_emitted".to_string()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "MI_". For example "MI_DATABASE_URL"
    /// will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("RI").separator("__"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn operation_fund_key(&self) -> anyhow::Result<String> {
        match (self.mode, self.operation_fund_key.clone()) {
            (Mode::Iot, None) => anyhow::bail!("operation fund key is required for IOT mode"),
            (Mode::Iot, Some(fund_key)) => Ok(fund_key),
            (Mode::Mobile, _) => Ok("".to_string()),
        }
    }
}
