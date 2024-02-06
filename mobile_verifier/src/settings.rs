use chrono::{DateTime, TimeZone, Utc};
use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Reward period in hours. (Default is 24)
    #[serde(default = "default_reward_period")]
    pub rewards: i64,
    #[serde(default = "default_reward_offset_minutes")]
    pub reward_offset_minutes: i64,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub data_transfer_ingest: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub price_tracker: price::price_tracker::Settings,
    pub config_client: mobile_config::ClientSettings,
    #[serde(default = "default_start_after")]
    pub start_after: u64,
    pub modeled_coverage_start: u64,
    /// Max distance in meters between the heartbeat and all of the hexes in
    /// its respective coverage object
    #[serde(default = "default_max_distance_from_coverage")]
    pub max_distance_from_coverage: u32,
    /// Max distance in meters between the asserted location of a WIFI hotspot
    /// and the lat/lng defined in a heartbeat
    /// beyond which its location weight will be reduced
    #[serde(default = "default_max_asserted_distance_deviation")]
    pub max_asserted_distance_deviation: u32,
    // Geofencing settings
    pub wifi_geofence_regions: String,
    #[serde(default = "default_fencing_resolution")]
    pub wifi_fencing_resolution: u8,
    pub cbrs_geofence_regions: String,
    #[serde(default = "default_fencing_resolution")]
    pub cbrs_fencing_resolution: u8,
}

fn default_fencing_resolution() -> u8 {
    7
}

pub fn default_max_distance_from_coverage() -> u32 {
    // Default is 2 km
    2000
}

pub fn default_max_asserted_distance_deviation() -> u32 {
    100
}

pub fn default_log() -> String {
    "mobile_verifier=debug,poc_store=info".to_string()
}

pub fn default_start_after() -> u64 {
    0
}

pub fn default_reward_period() -> i64 {
    24
}

pub fn default_reward_offset_minutes() -> i64 {
    30
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

    pub fn start_after(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.start_after as i64, 0)
            .single()
            .unwrap()
    }

    pub fn modeled_coverage_start(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.modeled_coverage_start as i64, 0)
            .single()
            .unwrap()
    }

    pub fn wifi_region_paths(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let paths = std::fs::read_dir(&self.wifi_geofence_regions)?;
        Ok(paths
            .into_iter()
            .collect::<Result<Vec<std::fs::DirEntry>, std::io::Error>>()?
            .into_iter()
            .map(|path| path.path())
            .collect())
    }

    pub fn wifi_fencing_resolution(&self) -> anyhow::Result<h3o::Resolution> {
        Ok(h3o::Resolution::try_from(self.wifi_fencing_resolution)?)
    }

    pub fn cbrs_region_paths(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let paths = std::fs::read_dir(&self.cbrs_geofence_regions)?;
        Ok(paths
            .into_iter()
            .collect::<Result<Vec<std::fs::DirEntry>, std::io::Error>>()?
            .into_iter()
            .map(|path| path.path())
            .collect())
    }

    pub fn cbrs_fencing_resolution(&self) -> anyhow::Result<h3o::Resolution> {
        Ok(h3o::Resolution::try_from(self.cbrs_fencing_resolution)?)
    }
}
