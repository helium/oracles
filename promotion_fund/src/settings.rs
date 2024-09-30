use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use humantime_serde::re::humantime;

#[derive(Debug, serde::Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string.
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Temporary storage before writing to S3
    pub file_sink_cache: PathBuf,
    /// How often to check for updates of service provider promotion values from
    /// solana. (default: 6 hours)
    #[serde(with = "humantime_serde", default = "default_solana_check_interval")]
    pub solana_check_interval: Duration,
    /// How far back do we go looking for the latest values in s3 on startup. If
    /// there become many files, update this value to a window just past the
    /// most recent. Value only used on startup.
    /// (default: unix epoch)
    #[serde(default = "default_lookback_start_after")]
    pub lookback_start_after: DateTime<Utc>,
    /// Solana RPC settings
    pub solana: solana::carrier::Settings,
    /// File Store Bucket Settings
    pub file_store_output: file_store::Settings,
    /// Metrics Settings
    pub metrics: poc_metrics::Settings,
}

fn default_log() -> String {
    "promotion_fund=info".to_string()
}

fn default_solana_check_interval() -> Duration {
    humantime::parse_duration("6 hours").unwrap()
}

fn default_lookback_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "PROMO_". For example
    /// "PROMO_LOG" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }

        builder
            .add_source(Environment::with_prefix("PROMO").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
