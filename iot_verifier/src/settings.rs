use chrono::Duration;
use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::Path;
use tokio::time;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "iot_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// the base_stale period in seconds
    /// if this is set, this value will be added to the entropy and report
    /// stale periods and is to prevent data being unnecessarily purged
    /// in the event the verifier is down for an extended period of time
    #[serde(default = "default_base_stale_period")]
    pub base_stale_period: i64,
    pub database: db_store::Settings,
    pub follower: node_follower::Settings,
    pub ingest: file_store::Settings,
    pub entropy: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub density_scaler: density_scaler::Settings,
    pub denylist: denylist::Settings,
    /// Reward period in hours. (Default to 24)
    #[serde(default = "default_reward_period")]
    pub rewards: i64,
    /// Reward calculation offset in minutes, rewards will be calculated at the end
    /// of the reward period + reward_offset_minutes
    #[serde(default = "default_reward_offset_minutes")]
    pub reward_offset_minutes: i64,
    #[serde(default = "default_max_witnesses_per_poc")]
    pub max_witnesses_per_poc: u64,
    /// The cadence at which hotspots are permitted to beacon (in seconds)
    #[serde(default = "default_beacon_interval")]
    pub beacon_interval: i64,
    /// Tolerance applied to beacon intervals within which beacons will be accepted (in seconds)
    #[serde(default = "default_beacon_interval_tolerance")]
    pub beacon_interval_tolerance: i64,
    /// window width for the entropy and poc report loader ( in seconds )
    /// each poll the loader will load reports from start time to start time + window width
    #[serde(default = "default_poc_loader_window_width")]
    pub poc_loader_window_width: i64,
    /// max window age for the entropy and poc report loader ( in seconds )
    /// the starting point of the window will never be older than now - max age
    #[serde(default = "default_poc_loader_window_max_lookback_age")]
    pub poc_loader_window_max_lookback_age: i64,
    /// cadence for how often to look for poc reports from s3 buckets
    #[serde(default = "default_poc_loader_poll_time")]
    pub poc_loader_poll_time: u64,
    /// cadence for how often to look for entropy reports from s3 buckets
    #[serde(default = "default_poc_loader_entropy_poll_time")]
    pub poc_loader_entropy_poll_time: u64,
    /// the lifespan of a piece of entropy
    #[serde(default = "default_poc_entropy_lifespan ")]
    pub poc_entropy_lifespan: i64,
}

// Default: 3 minutes
pub fn default_poc_entropy_lifespan() -> i64 {
    3 * 60
}

// Default: 3 minutes
pub fn default_poc_loader_entropy_poll_time() -> u64 {
    3 * 60
}

// Default: 5 minutes
pub fn default_poc_loader_poll_time() -> u64 {
    5 * 60
}

// Default: 60 minutes
// this should be at least poc_loader_window_width * 2
pub fn default_poc_loader_window_max_lookback_age() -> i64 {
    60 * 60
}

// Default: 15 minutes
pub fn default_poc_loader_window_width() -> i64 {
    15 * 60
}

// Default: 10 minutes
pub fn default_beacon_interval_tolerance() -> i64 {
    10 * 60
}

// Default: 6 hours
pub fn default_beacon_interval() -> i64 {
    6 * 60 * 60
}

pub fn default_log() -> String {
    "iot_verifier=debug,poc_store=info".to_string()
}

pub fn default_base_stale_period() -> i64 {
    0
}

fn default_reward_period() -> i64 {
    24
}

fn default_reward_offset_minutes() -> i64 {
    30
}

pub fn default_max_witnesses_per_poc() -> u64 {
    14
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
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

    pub fn reward_offset_duration(&self) -> Duration {
        Duration::minutes(self.reward_offset_minutes)
    }

    pub fn beacon_interval(&self) -> Duration {
        Duration::seconds(self.beacon_interval)
    }

    pub fn beacon_interval_tolerance(&self) -> Duration {
        Duration::seconds(self.beacon_interval_tolerance)
    }

    pub fn poc_loader_window_width(&self) -> Duration {
        Duration::seconds(self.poc_loader_window_width)
    }

    pub fn poc_loader_window_max_lookback_age(&self) -> Duration {
        Duration::seconds(self.poc_loader_window_max_lookback_age)
    }

    pub fn poc_loader_poll_time(&self) -> time::Duration {
        time::Duration::from_secs(self.poc_loader_poll_time)
    }

    pub fn poc_loader_entropy_poll_time(&self) -> time::Duration {
        time::Duration::from_secs(self.poc_loader_entropy_poll_time)
    }

    pub fn poc_entropy_lifespan(&self) -> Duration {
        Duration::seconds(self.poc_entropy_lifespan)
    }
}
