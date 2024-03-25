use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "poc_entropy=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Reward files check interval in seconds. (Default is 15 minutes)
    #[serde(with = "humantime_serde", default = "default_reward_check_interval")]
    pub reward_check_interval: Duration,
    /// Hex Activation check  interval in seconds. (Default is 15 minutes)
    /// determines how often we will check the DB for queued txns to solana
    #[serde(
        with = "humantime_serde",
        default = "default_activation_check_interval"
    )]
    pub activation_check_interval: Duration,
    pub database: db_store::Settings,
    pub verifier: file_store::Settings,
    pub mobile_config_client: mobile_config::ClientSettings,
    pub metrics: poc_metrics::Settings,
    pub output: file_store::Settings,
    #[serde(default)]
    pub enable_solana_integration: bool,
    pub solana: Option<solana::start_boost::Settings>,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    // the number of records to fit per solana txn
    #[serde(default = "default_txn_batch_size")]
    pub txn_batch_size: u32,
    // default retention period in seconds
    #[serde(with = "humantime_serde", default = "default_retention_period")]
    pub retention_period: Duration,
    #[serde(with = "humantime_serde", default = "default_txn_retry_delay")]
    txn_retry_delay: Duration,
}

fn default_txn_retry_delay() -> Duration {
    humantime::parse_duration("1 second").unwrap()
}

fn default_retention_period() -> Duration {
    humantime::parse_duration("7 days").unwrap()
}

fn default_txn_batch_size() -> u32 {
    18
}

fn default_reward_check_interval() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_activation_check_interval() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_log() -> String {
    "boost_manager=info".to_string()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
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
            .add_source(Environment::with_prefix("MI").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn txn_batch_size(&self) -> usize {
        self.txn_batch_size as usize
    }
}
