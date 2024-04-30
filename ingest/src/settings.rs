use config::{Config, Environment, File};
use helium_crypto::Network;
use serde::Deserialize;
use std::{
    net::{AddrParseError, SocketAddr},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default
    /// "ingest=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// File name to be watch by custom tracing
    #[serde(default = "default_tracing")]
    pub tracing: String,
    /// Mode to run the server in (iot or mobile). Required
    pub mode: Mode,
    /// Listen address. Required. Default is 0.0.0.0:9081
    #[serde(default = "default_listen_addr")]
    pub listen: String,
    /// Local folder for storing intermediate files
    pub cache: String,
    /// Network required in all public keys:  mainnet | testnet
    pub network: Network,
    /// Timeout of session key offer in seconds
    #[serde(default = "default_session_key_offer_timeout")]
    pub session_key_offer_timeout: u64,
    /// Timeout of session key session in seconds
    #[serde(default = "default_session_key_timeout")]
    pub session_key_timeout: u64,
    /// Settings for exposed public API
    /// Target bucket for uploads
    pub output: file_store::Settings,
    /// API token required as part of a Bearer authentication GRPC request
    /// header. Used only by the mobile mode currently
    pub token: Option<String>,
    /// Target output bucket details Metrics settings
    pub metrics: poc_metrics::Settings,
}

pub fn default_session_key_timeout() -> u64 {
    30 * 60
}

pub fn default_session_key_offer_timeout() -> u64 {
    5
}

pub fn default_listen_addr() -> String {
    "0.0.0.0:9081".to_string()
}

pub fn default_log() -> String {
    "ingest=debug,poc_store=info".to_string()
}

pub fn default_tracing() -> String {
    "tracing".to_string()
}

pub fn default_sink() -> String {
    "/var/data/ingest".to_string()
}

/// Mode to deploy the ingest engine in. Each mode exposes different submission
/// grpc methods
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Iot,
    Mobile,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
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
            .add_source(Environment::with_prefix("INGEST").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.listen)
    }

    pub fn session_key_offer_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.session_key_offer_timeout)
    }

    pub fn session_key_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.session_key_timeout)
    }
}
