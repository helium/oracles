use config::{Config, Environment, File};
use helium_crypto::Network;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path, path::PathBuf, time::Duration};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default
    /// "ingest=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Mode to run the server in (iot or mobile). Required
    pub mode: Mode,
    /// Listen address. Required. Default is 0.0.0.0:9081
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,
    /// Local folder for storing intermediate files
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Network required in all public keys:  mainnet | testnet
    #[serde(default = "default_network", skip_serializing)]
    pub network: Network,
    /// Timeout of session key offer in seconds
    #[serde(
        with = "humantime_serde",
        default = "default_session_key_offer_timeout"
    )]
    pub session_key_offer_timeout: Duration,
    /// Timeout of session key session in seconds
    #[serde(with = "humantime_serde", default = "default_session_key_timeout")]
    pub session_key_timeout: Duration,
    #[serde(default)]
    pub file_store: file_store::Settings,
    pub output_bucket: String,
    /// Timeout of session key session in seconds
    #[serde(with = "humantime_serde", default = "default_roll_time")]
    pub roll_time: Duration,
    /// API token required as part of a Bearer authentication GRPC request
    /// header. Used only by the mobile mode currently
    #[serde(skip_serializing)]
    pub token: Option<String>,
    /// Target output bucket details Metrics settings
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    // mobile config client settings
    // optional to avoid having to define a client for IOT mode
    pub config_client: Option<mobile_config::ClientSettings>,
    /// Key that can sign Chain Rewardable Entities messages
    pub chain_rewardable_entities_auth_key: Option<String>,
}

fn default_network() -> Network {
    Network::MainNet
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/ingest/data")
}

fn default_roll_time() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_session_key_timeout() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

fn default_session_key_offer_timeout() -> Duration {
    humantime::parse_duration("5 seconds").unwrap()
}

fn default_listen_addr() -> SocketAddr {
    "0.0.0.0:9081".parse().unwrap()
}

fn default_log() -> String {
    "ingest=debug,poc_store=info".to_string()
}

/// Mode to deploy the ingest engine in. Each mode exposes different submission
/// grpc methods
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Iot,
    Mobile,
    Chain,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "ENTROPY_". For example
    /// "ENTROPY_LOG" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(
                Environment::with_prefix("INGEST")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
