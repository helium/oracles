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
    pub iot_config_client: iot_config::client::Settings,
    pub ingest: file_store::Settings,
    pub packet_ingest: file_store::Settings,

    pub entropy: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub denylist: denylist::Settings,
    pub price_tracker: price::price_tracker::Settings,
    /// Reward period in hours. (Default to 24)
    #[serde(default = "default_reward_period")]
    pub rewards: i64,
    /// Reward calculation offset in minutes, rewards will be calculated at the end
    /// of the reward period + reward_offset_minutes
    #[serde(default = "default_reward_offset_minutes")]
    pub reward_offset_minutes: i64,
    #[serde(default = "default_max_witnesses_per_poc")]
    pub max_witnesses_per_poc: u64,
    /// The cadence at which hotspots are permitted to beacon (in hours)
    /// this should be a factor of 24 so that we can have clear
    /// beaconing bucket sizes
    #[serde(default = "default_beacon_interval")]
    pub beacon_interval: u64,
    /// Trigger interval for generating a transmit scaling map
    #[serde(default = "default_transmit_scale_interval")]
    pub transmit_scale_interval: i64,
    // roll up time defined in the ingestors ( in seconds )
    // ie the time after which they will write out files to s3
    // this will be used when padding out the witness
    // loader window before and after values
    #[serde(default = "default_ingestor_rollup_time")]
    pub ingestor_rollup_time: i64,
    /// window width for the poc report loader ( in seconds )
    /// each poll the loader will load reports from start time to start time + window width
    /// NOTE: the window width should be as a minimum equal to the ingestor roll up period
    ///       any less and the verifier will potentially miss incoming reports
    #[serde(default = "default_poc_loader_window_width")]
    pub poc_loader_window_width: i64,
    /// cadence for how often to look for poc reports from s3 buckets
    #[serde(default = "default_poc_loader_poll_time")]
    pub poc_loader_poll_time: u64,
    /// the lifespan of a piece of entropy
    #[serde(default = "default_entropy_lifespan ")]
    pub entropy_lifespan: i64,
    /// max window age for the poc report loader ( in seconds )
    /// the starting point of the window will never be older than now - max age
    #[serde(default = "default_loader_window_max_lookback_age")]
    pub loader_window_max_lookback_age: i64,
    /// File store poll interval for incoming entropy reports, in seconds
    #[serde(default = "default_entropy_interval")]
    pub entropy_interval: i64,
    /// File store poll interval for incoming packets, in seconds. (Default is 900; 15 minutes)
    #[serde(default = "default_packet_interval")]
    pub packet_interval: i64,
    /// the max number of times a beacon report will be retried
    /// after this the report will be ignored and eventually be purged
    #[serde(default = "default_beacon_max_retries")]
    pub beacon_max_retries: u64,
    /// the max number of times a witness report will be retried
    /// after this the report will be ignored and eventually be purged
    #[serde(default = "default_witness_max_retries")]
    pub witness_max_retries: u64,
    /// interval at which gateways are refreshed
    #[serde(default = "default_gateway_refresh_interval")]
    pub gateway_refresh_interval: i64,
    /// interval at which region params in the cache are refreshed
    #[serde(default = "default_region_params_refresh_interval")]
    pub region_params_refresh_interval: u64,
}

// Default: 30 minutes
fn default_gateway_refresh_interval() -> i64 {
    30 * 60
}

// Default: 30 minutes
fn default_region_params_refresh_interval() -> u64 {
    30 * 60
}

// Default: 60 minutes
// this should be at least poc_loader_window_width * 2
pub fn default_loader_window_max_lookback_age() -> i64 {
    60 * 60
}

// Default: 5 minutes
fn default_entropy_interval() -> i64 {
    5 * 60
}

// Default: 5 minutes
pub fn default_entropy_lifespan() -> i64 {
    5 * 60
}

// Default: 5 minutes
pub fn default_poc_loader_window_width() -> i64 {
    5 * 60
}

// Default: 5 minutes
pub fn default_ingestor_rollup_time() -> i64 {
    5 * 60
}
// Default: 5 minutes
// in normal operational mode the poll time should be set same as that of the window width
// however, if for example we are loading historic data, ie looking back 24hours, we will want
// the loader to be catching up as quickly as possible and so we will want to poll more often
// in order to iterate quickly over the historic data
// the average time it takes to load the data available within with window width needs to be
// considered here
pub fn default_poc_loader_poll_time() -> u64 {
    5 * 60
}

// Default: 6 hours
pub fn default_beacon_interval() -> u64 {
    6 * 60 * 60
}

// Default: 30 min
pub fn default_transmit_scale_interval() -> i64 {
    1800
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

fn default_packet_interval() -> i64 {
    900
}

// runner runs at 30 sec intervals
// 60 permits retries for up to 30 mins
fn default_beacon_max_retries() -> u64 {
    60
}

// witnesses are processed per beacon
// as such the retry can be much less as if the beacon fails
// then the witnesses auto fail too
// if the beacon is processed successfully, then any one witness
// can only fail 5 times before we move on without it
fn default_witness_max_retries() -> u64 {
    5
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

    pub fn poc_loader_window_width(&self) -> Duration {
        Duration::seconds(self.poc_loader_window_width)
    }

    pub fn ingestor_rollup_time(&self) -> Duration {
        Duration::seconds(self.ingestor_rollup_time)
    }

    pub fn poc_loader_poll_time(&self) -> time::Duration {
        time::Duration::from_secs(self.poc_loader_poll_time)
    }

    pub fn loader_window_max_lookback_age(&self) -> Duration {
        Duration::seconds(self.loader_window_max_lookback_age)
    }

    pub fn entropy_lifespan(&self) -> Duration {
        Duration::seconds(self.entropy_lifespan)
    }

    pub fn base_stale_period(&self) -> Duration {
        Duration::seconds(self.base_stale_period)
    }

    pub fn entropy_interval(&self) -> Duration {
        Duration::seconds(self.entropy_interval)
    }
    pub fn packet_interval(&self) -> Duration {
        Duration::seconds(self.packet_interval)
    }
    pub fn gateway_refresh_interval(&self) -> Duration {
        Duration::seconds(self.gateway_refresh_interval)
    }
    pub fn region_params_refresh_interval(&self) -> time::Duration {
        time::Duration::from_secs(self.region_params_refresh_interval)
    }
}
