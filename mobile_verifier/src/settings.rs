use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Reward period in hours. (Default is 24 hours)
    #[serde(with = "humantime_serde", default = "default_reward_period")]
    pub reward_period: Duration,
    #[serde(with = "humantime_serde", default = "default_reward_period_offset")]
    pub reward_period_offset: Duration,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub data_transfer_ingest: file_store::Settings,
    pub output: file_store::Settings,
    /// S3 bucket from which new data sets are downloaded for oracle boosting
    /// assignments
    pub data_sets: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub price_tracker: price::price_tracker::Settings,
    pub config_client: mobile_config::ClientSettings,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    pub modeled_coverage_start: DateTime<Utc>,
    /// Max distance in meters between the heartbeat and all of the hexes in
    /// its respective coverage object
    #[serde(default = "default_max_distance_from_coverage")]
    pub max_distance_from_coverage: u32,
    /// Max distance in meters between the asserted location of a WIFI hotspot
    /// and the lat/lng defined in a heartbeat
    /// beyond which its location weight will be reduced
    #[serde(default = "default_max_asserted_distance_deviation")]
    pub max_asserted_distance_deviation: u32,
    /// Directory in which new oracle boosting data sets are downloaded into
    pub data_sets_directory: PathBuf,
    // Geofencing settings
    pub usa_and_mexico_geofence_regions: String,
    #[serde(default = "default_fencing_resolution")]
    pub usa_and_mexico_fencing_resolution: u8,
    pub usa_geofence_regions: String,
    #[serde(default = "default_fencing_resolution")]
    pub usa_fencing_resolution: u8,
}

fn default_fencing_resolution() -> u8 {
    7
}

fn default_max_distance_from_coverage() -> u32 {
    // Default is 2 km
    2000
}

fn default_max_asserted_distance_deviation() -> u32 {
    100
}

fn default_log() -> String {
    "mobile_verifier=debug,poc_store=info".to_string()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_reward_period() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_reward_period_offset() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
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

    pub fn usa_region_paths(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let paths = std::fs::read_dir(&self.usa_geofence_regions)?;
        Ok(paths
            .into_iter()
            .collect::<Result<Vec<std::fs::DirEntry>, std::io::Error>>()?
            .into_iter()
            .map(|path| path.path())
            .collect())
    }

    pub fn usa_fencing_resolution(&self) -> anyhow::Result<h3o::Resolution> {
        Ok(h3o::Resolution::try_from(self.usa_fencing_resolution)?)
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
