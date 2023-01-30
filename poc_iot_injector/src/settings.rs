use chrono::Duration as ChronoDuration;
use config::{Config, Environment, File};
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "poc_iot_injector=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// File to load keypair from
    pub keypair: String,
    /// Trigger interval in seconds. (Default is 1800; 30 minutes)
    #[serde(default = "default_trigger_interval")]
    pub trigger: u64,
    /// Last PoC submission timestamp in seconds since unix epoch. (Default is
    /// unix epoch)
    #[serde(default = "default_last_poc_submission")]
    pub last_poc_submission: i64,
    #[serde(default = "default_do_submission")]
    /// Whether to submit txns to mainnet, default: false
    pub do_submission: bool,
    pub database: db_store::Settings,
    pub transactions: node_follower::Settings,
    pub verifier: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    #[serde(default = "default_max_witnesses_per_receipt")]
    pub max_witnesses_per_receipt: u64,
    /// Offset receipt submission to wait for S3 files being written (secs)
    /// Receipts would be submitted from last_poc_submission_ts to utc::now - submission_offset
    /// Default = 5 mins
    #[serde(default = "default_submission_offset")]
    pub submission_offset: i64,
}

pub fn default_log() -> String {
    "poc_iot_injector=debug,poc_store=info".to_string()
}

pub fn default_last_poc_submission() -> i64 {
    0
}

pub fn default_do_submission() -> bool {
    false
}

fn default_trigger_interval() -> u64 {
    1800
}

fn default_submission_offset() -> i64 {
    5 * 60
}

pub fn default_max_witnesses_per_receipt() -> u64 {
    14
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "INJECT_". For example
    /// "INJECT_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of INJECT)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("INJECT").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn keypair(&self) -> Result<helium_crypto::Keypair, Box<helium_crypto::Error>> {
        let data = std::fs::read(&self.keypair).map_err(helium_crypto::Error::from)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }

    pub fn trigger_interval(&self) -> Duration {
        Duration::from_secs(self.trigger)
    }

    pub fn submission_offset(&self) -> ChronoDuration {
        ChronoDuration::seconds(self.submission_offset)
    }
}
