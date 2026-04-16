use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::banning;

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Cache location for generated verified reports
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Burn period in hours. (Default is 1 hour)
    #[serde(with = "humantime_serde", default = "default_burn_period")]
    pub burn_period: Duration,
    /// Minimum burn period when error. (Default is 15 minutes)
    #[serde(with = "humantime_serde", default = "default_min_burn_period")]
    pub min_burn_period: Duration,
    pub database: db_store::Settings,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    pub ingest_bucket: file_store::BucketSettings,
    pub output_bucket: file_store::BucketSettings,
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
    /// When a burn transaction is not a success, how many times should
    /// we try to confirm the transaction before considering it a failure?
    #[serde(default = "default_txn_confirmation_retry_attempts")]
    pub txn_confirmation_retry_attempts: usize,
    /// When a burn transaction is not a success, how long should we
    /// wait between trying to confirm if the transaction made it to Solana?
    #[serde(
        with = "humantime_serde",
        default = "default_txn_confirmation_check_interval"
    )]
    pub txn_confirmation_check_interval: Duration,

    /// Settings for Banning
    pub banning: banning::BanSettings,

    pub iceberg_settings: Option<helium_iceberg::Settings>,

    /// Settings for Iceberg backfill. Only used when iceberg_settings is configured.
    /// Backfill processes VerifiedDataTransferSession and ValidDataTransferSession files
    /// from `backfill.start_after` up to `backfill.stop_after` (the Iceberg deployment date).
    /// When absent, both backfillers are created as no-ops and no file pollers are started.
    #[serde(default)]
    pub backfill: Option<BackfillSettings>,
}

/// Settings controlling the Iceberg backfill window.
///
/// Backfill covers [`start_after`, `stop_after`). Set `stop_after` to the date
/// Iceberg was first enabled in production. Files before that date are written by
/// the backfiller; files on or after are written by the daemon's real-time path.
#[derive(Debug, Deserialize, Serialize)]
pub struct BackfillSettings {
    /// Start of the backfill window. Defaults to UNIX_EPOCH (backfill all available history).
    #[serde(default = "default_backfill_start_after")]
    pub start_after: DateTime<Utc>,
    /// End of the backfill window (exclusive). Must be set to the Iceberg deployment date
    /// to prevent the backfiller from overlapping with the daemon's real-time Iceberg writes.
    pub stop_after: DateTime<Utc>,
}

fn default_backfill_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_purger_interval() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_purger_max_age() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_txn_confirmation_check_interval() -> Duration {
    humantime::parse_duration("1 min").unwrap()
}

fn default_txn_confirmation_retry_attempts() -> usize {
    5
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_log() -> String {
    "mobile_packet_verifier=debug,poc_store=info".to_string()
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/mobile-packet-verifier/data")
}

fn default_burn_period() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_min_burn_period() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
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
            .add_source(
                Environment::with_prefix("MPV")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
