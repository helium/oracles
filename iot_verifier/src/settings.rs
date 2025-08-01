use anyhow::bail;
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "iot_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Cache location for generated verified reports
    pub cache: String,

    /// the base_stale period
    /// this is used to determine the minimum period for
    /// entropy, witness and beacon reports after which they will be deemed stale
    /// and considered for purging by the purger
    /// if the verifier is down for an extended period of time, extend this value
    /// to prevent data being unnecessarily purged and unprocessed historical data being lost
    #[serde(with = "humantime_serde", default = "default_base_stale_period")]
    pub base_stale_period: Duration,

    /// beacon_stale_period is added to the base stale period
    /// to determine the final stale period for beacon reports
    /// each report type has its own stale period to enable report specific
    /// stale periods
    /// TODO: this fine grained control is overkill - one stale period to rule them all is sufficient
    #[serde(with = "humantime_serde", default = "default_beacon_stale_period")]
    pub beacon_stale_period: Duration,

    /// witness_stale_period is added to the base stale period
    /// to determine the final stale period for witness reports
    /// each report type has its own stale period to enable report specific
    /// stale periods
    /// TODO: this fine grained control is overkill - one stale period to rule them all is sufficient
    #[serde(with = "humantime_serde", default = "default_witness_stale_period")]
    pub witness_stale_period: Duration,

    /// entropy_stale_period is added to the base stale period
    /// to determine the final stale period for entropy reports
    /// each report type has its own stale period to enable report specific
    /// stale periods
    /// TODO: this fine grained control is overkill - one stale period to rule them all is sufficient
    #[serde(with = "humantime_serde", default = "default_entropy_stale_period")]
    pub entropy_stale_period: Duration,

    pub database: db_store::Settings,
    pub iot_config_client: iot_config::client::Settings,
    pub ingest: file_store::Settings,
    pub packet_ingest: file_store::Settings,
    pub entropy: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub denylist: denylist::Settings,
    pub price_tracker: price::price_tracker::Settings,

    /// Reward period in hours
    #[serde(with = "humantime_serde", default = "default_reward_period")]
    pub reward_period: Duration,

    /// Reward calculation offset in minutes, rewards will be calculated at the end
    /// of the reward_period + reward_period_offset
    #[serde(with = "humantime_serde", default = "default_reward_period_offset")]
    pub reward_period_offset: Duration,

    #[serde(default = "default_max_witnesses_per_poc")]
    pub max_witnesses_per_poc: u64,

    /// Gateways are permitted to beacon at a cadence of 1 per beacon_interval
    /// Think of this interval as a window in which a gateway can beacon once
    /// and only once at any point within any one window
    /// this should be a factor of 24 so that we can have evenly spread
    /// beaconing window sizes
    #[serde(with = "humantime_serde", default = "default_beacon_interval")]
    pub beacon_interval: Duration,

    /// roll up time defined in the ingestors
    /// ie the time after which they will write out files to s3
    /// this will be used when padding out the witness
    /// loader window before and after values
    #[serde(with = "humantime_serde", default = "default_ingestor_rollup_time")]
    pub ingestor_rollup_time: Duration,

    /// window width for the poc report loader
    /// NOTE: the window width should be as a minimum equal to the ingestor roll up period
    ///       any less and the verifier will potentially miss incoming reports
    #[serde(with = "humantime_serde", default = "default_poc_loader_window_width")]
    pub poc_loader_window_width: Duration,

    /// cadence for how often to look for poc reports from s3 buckets
    #[serde(with = "humantime_serde", default = "default_poc_loader_poll_time")]
    pub poc_loader_poll_time: Duration,

    /// max window age for the poc report loader
    /// the starting point of the window will never be older than now - max age
    /// if ingesting historic data, this value should be set appropriately
    /// NOTE: this was added mostly to support running the verifier locally
    /// and doing so intermittently which results in the verifier being behind
    /// with this setting it ensures we can ignore data older than the max lookback age
    /// TODO: this setting should be considered for removal in the future
    #[serde(
        with = "humantime_serde",
        default = "default_loader_window_max_lookback_age"
    )]
    pub loader_window_max_lookback_age: Duration,

    /// File store poll interval for incoming entropy reports
    #[serde(with = "humantime_serde", default = "default_entropy_interval")]
    pub entropy_interval: Duration,

    /// File store poll interval for incoming packets, in seconds
    #[serde(with = "humantime_serde", default = "default_packet_interval")]
    pub packet_interval: Duration,

    /// the max number of times a beacon report will be retried
    /// after which the report will be ignored and eventually be purged
    #[serde(default = "default_beacon_max_retries")]
    pub beacon_max_retries: u64,

    /// the max number of times a witness report will be retried
    /// after which the report will be ignored and eventually be purged
    #[serde(default = "default_witness_max_retries")]
    pub witness_max_retries: u64,

    /// interval at which cached gateways are refreshed from iot config
    #[serde(with = "humantime_serde", default = "default_gateway_refresh_interval")]
    pub gateway_refresh_interval: Duration,

    /// interval at which cached region params are refreshed from iot config
    #[serde(
        with = "humantime_serde",
        default = "default_region_params_refresh_interval"
    )]
    pub region_params_refresh_interval: Duration,
}

fn default_gateway_refresh_interval() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

fn default_region_params_refresh_interval() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

// this should be at least poc_loader_window_width * 2
fn default_loader_window_max_lookback_age() -> Duration {
    humantime::parse_duration("60 minutes").unwrap()
}

fn default_entropy_interval() -> Duration {
    humantime::parse_duration("5 minutes").unwrap()
}

fn default_poc_loader_window_width() -> Duration {
    humantime::parse_duration("5 minutes").unwrap()
}

fn default_ingestor_rollup_time() -> Duration {
    humantime::parse_duration("5 minutes").unwrap()
}

// in normal operational mode the poll time should be set same as that of the window width
// however, if for example we are loading historic data, ie looking back 24hours, we will want
// the loader to be catching up as quickly as possible and so we will want to poll more often
// in order to iterate quickly over the historic data
// the average time it takes to load the data available within with window width needs to be
// considered here
fn default_poc_loader_poll_time() -> Duration {
    humantime::parse_duration("5 minutes").unwrap()
}

fn default_beacon_interval() -> Duration {
    humantime::parse_duration("6 hours").unwrap()
}

fn default_log() -> String {
    "iot_verifier=debug,poc_store=info".to_string()
}

fn default_base_stale_period() -> Duration {
    Duration::default()
}

fn default_beacon_stale_period() -> Duration {
    humantime::parse_duration("45 minutes").unwrap()
}

fn default_witness_stale_period() -> Duration {
    humantime::parse_duration("45 minutes").unwrap()
}

fn default_entropy_stale_period() -> Duration {
    humantime::parse_duration("60 minutes").unwrap()
}

fn default_reward_period() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_reward_period_offset() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

fn default_max_witnesses_per_poc() -> u64 {
    14
}

fn default_packet_interval() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
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
    /// Environment overrides have the same name as the entries in the settings
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

    pub fn beacon_interval(&self) -> anyhow::Result<Duration> {
        // validate the beacon_interval value is a factor of 24, if not bail out
        if (24 * 60 * 60) % self.beacon_interval.as_secs() != 0 {
            bail!("beacon interval is not a factor of 24")
        } else {
            Ok(self.beacon_interval)
        }
    }
}
