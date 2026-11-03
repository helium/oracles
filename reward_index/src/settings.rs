use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "poc_entropy=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    #[serde(default = "default_unallocated_reward_entity_key")]
    pub unallocated_reward_entity_key: String,
    pub database: db_store::Settings,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    pub iceberg: IcebergSettings,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IcebergSettings {
    pub catalog: helium_iceberg::Settings,
    /// `start_period` of the first reward epoch to index from Iceberg. Rows for
    /// earlier epochs are skipped; they were indexed from reward manifest files
    /// before reward-index read Iceberg.
    pub start_epoch_period: DateTime<Utc>,
    /// Only snapshots committed after this time are read on first start (before
    /// any watermark exists). Must be before the commit time of the
    /// `start_epoch_period` snapshots; anything earlier is skipped by
    /// `start_epoch_period`. Defaults to reading every snapshot.
    #[serde(default = "default_start_after")]
    pub snapshot_start_after: DateTime<Utc>,
    /// Directory for the stream watermark dbs. Mount on a PVC. Defaults to
    /// `$ICEBERG_STREAM_DB_DIR`, falling back to the current directory.
    pub db_dir: Option<PathBuf>,
    /// How often to poll the tables for new snapshots. (Default is 5 minutes)
    #[serde(with = "humantime_serde", default = "default_iceberg_poll_duration")]
    pub poll_duration: Duration,
}

fn default_iceberg_poll_duration() -> Duration {
    humantime::parse_duration("5 minutes").unwrap()
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
    /// file in uppercase and prefixed with "RI__". For example "RI__DATABASE_URL"
    /// will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `RI__DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("RI").separator("__"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
