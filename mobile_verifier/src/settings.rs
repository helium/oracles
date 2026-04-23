use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Buckets {
    pub ingest: file_store::BucketSettings,
    pub data_transfer: file_store::BucketSettings,
    pub data_sets: file_store::BucketSettings,
    pub output: file_store::BucketSettings,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    pub buckets: Buckets,
    /// Cache location for generated verified reports
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Reward period in hours. (Default is 24 hours)
    #[serde(with = "humantime_serde", default = "default_reward_period")]
    pub reward_period: Duration,
    #[serde(with = "humantime_serde", default = "default_reward_period_offset")]
    pub reward_period_offset: Duration,
    pub database: db_store::Settings,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    pub price_tracker: price_tracker::Settings,
    pub config_client: mobile_config::ClientSettings,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    /// Max distance in meters between the heartbeat and all of the hexes in
    /// its respective coverage object
    #[serde(default = "default_max_distance_from_coverage")]
    pub max_distance_from_coverage: u32,
    /// Directory in which new oracle boosting data sets are downloaded into
    #[serde(default = "default_data_sets_directory")]
    pub data_sets_directory: PathBuf,
    /// Poll duration for new data sets
    #[serde(with = "humantime_serde", default = "default_data_sets_poll_duration")]
    pub data_sets_poll_duration: Duration,
    pub iceberg_settings: Option<helium_iceberg::Settings>,
    /// Settings for speedtest Iceberg backfill. Only used when iceberg_settings is configured.
    /// Backfill processes VerifiedSpeedtest files from `speedtest_backfill.start_after` up to
    /// `speedtest_backfill.stop_after` (the Iceberg deployment date). When absent, the backfiller
    /// is a no-op and no file poller is started.
    #[serde(default)]
    pub speedtest_backfill: Option<BackfillSettings>,
    // Geofencing settings
    #[serde(default = "default_usa_and_mexico_geofence_regions")]
    pub usa_and_mexico_geofence_regions: PathBuf,
    #[serde(default = "default_fencing_resolution")]
    pub usa_and_mexico_fencing_resolution: u8,
}

/// Settings controlling the Iceberg backfill window.
///
/// Backfill covers [start_after, stop_after). Set stop_after to the date Iceberg
/// was first enabled in production. Files before that date are written by the
/// backfiller; files on or after are written by the daemon's real-time path.
#[derive(Debug, Deserialize, Serialize)]
pub struct BackfillSettings {
    /// Start of the backfill window. Defaults to UNIX_EPOCH (backfill all available history).
    #[serde(default = "default_backfill_start_after")]
    pub start_after: DateTime<Utc>,
    /// End of the backfill window (exclusive). Must be set to the Iceberg deployment date
    /// to prevent the backfiller from overlapping with the daemon's real-time Iceberg writes.
    pub stop_after: DateTime<Utc>,
    /// Override the process name used to track backfill position in the database.
    /// Change this to force a full re-backfill without touching the database directly.
    /// When absent, each backfiller uses its own default name.
    #[serde(default)]
    pub process_name: Option<String>,
}

impl BackfillSettings {
    pub fn into_options(&self, default_name: impl Into<String>) -> crate::backfill::BackfillOptions {
        crate::backfill::BackfillOptions {
            process_name: self
                .process_name
                .clone()
                .unwrap_or_else(|| default_name.into()),
            start_after: self.start_after,
            stop_after: self.stop_after,
            poll_duration: None,
            idle_timeout: None,
        }
    }
}

fn default_backfill_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_fencing_resolution() -> u8 {
    7
}

fn default_max_distance_from_coverage() -> u32 {
    // Default is 3 km
    3000
}

fn default_log() -> String {
    "mobile_verifier=info,file_store=info".to_string()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_reward_period() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_reward_period_offset() -> Duration {
    humantime::parse_duration("60 minutes").unwrap()
}

fn default_data_sets_poll_duration() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/mobile-verifier/data")
}

fn default_data_sets_directory() -> PathBuf {
    PathBuf::from("/opt/mobile-verifier/data_sets")
}

fn default_usa_and_mexico_geofence_regions() -> PathBuf {
    PathBuf::from("/opt/mobile-verifier/geofence")
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
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
            .add_source(
                Environment::with_prefix("MV")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn usa_and_mexico_region_paths(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let paths = std::fs::read_dir(&self.usa_and_mexico_geofence_regions)?;
        Ok(paths
            .into_iter()
            .collect::<Result<Vec<std::fs::DirEntry>, std::io::Error>>()?
            .into_iter()
            .map(|path| path.path())
            .collect())
    }

    pub fn usa_and_mexico_fencing_resolution(&self) -> anyhow::Result<h3o::Resolution> {
        Ok(h3o::Resolution::try_from(
            self.usa_and_mexico_fencing_resolution,
        )?)
    }

    pub fn store_base_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.cache)
    }
}
