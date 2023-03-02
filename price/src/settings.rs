use anyhow::Result;
use chrono::Duration;
use config::{Config, Environment, File};
use helium_proto::BlockchainTokenTypeV1;
use serde::Deserialize;
use solana_program::pubkey::Pubkey as SolPubkey;
use std::{
    net::{AddrParseError, SocketAddr},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub hnt_price_key: Option<String>,
    pub mobile_price_key: Option<String>,
    pub iot_price_key: Option<String>,
    pub hst_price_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    /// Listen address for http requests for price. Default "0.0.0.0:8080"
    #[serde(default = "default_hnt_listen_addr")]
    pub hnt_listen: String,
    #[serde(default = "default_mobile_listen_addr")]
    pub mobile_listen: String,
    #[serde(default = "default_iot_listen_addr")]
    pub iot_listen: String,
    #[serde(default = "default_hst_listen_addr")]
    pub hst_listen: String,
    /// Source URL for price data. Required
    #[serde(default = "default_rpc_endpoint")]
    pub rpc_endpoint: String,
    /// Target output bucket details
    pub output: file_store::Settings,
    /// Folder for local cache of ingest data
    #[serde(default = "default_cache")]
    pub cache: String,
    /// Metrics settings
    pub metrics: poc_metrics::Settings,
    /// Sink roll time (mins). Default = 3 mins.
    #[serde(default = "default_sink_roll_mins")]
    pub sink_roll_mins: i64,
    /// Tick interval (secs). Default = 60s.
    #[serde(default = "default_tick_interval")]
    pub tick_interval: i64,
    /// Price age (get price as long as it was updated within `age` seconds of current time) (in secs).
    /// Default = 60s.
    #[serde(default = "default_age")]
    pub age: u64,
    /// Cluster Configuration
    pub cluster: ClusterConfig,
}

pub fn default_rpc_endpoint() -> String {
    "https://api.devnet.solana.com".to_string()
}

pub fn default_log() -> String {
    "price=debug".to_string()
}

pub fn default_sink_roll_mins() -> i64 {
    3
}

pub fn default_tick_interval() -> i64 {
    60
}

pub fn default_age() -> u64 {
    60
}

pub fn default_cache() -> String {
    "/var/data/price".to_string()
}

pub fn default_hnt_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

pub fn default_mobile_listen_addr() -> String {
    "0.0.0.0:8081".to_string()
}

pub fn default_iot_listen_addr() -> String {
    "0.0.0.0:8082".to_string()
}

pub fn default_hst_listen_addr() -> String {
    "0.0.0.0:8083".to_string()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "price_". For example
    /// "price_LOG_" will override the log setting.
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
            .add_source(Environment::with_prefix("price").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn hnt_listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.hnt_listen)
    }

    pub fn mobile_listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.mobile_listen)
    }

    pub fn iot_listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.iot_listen)
    }

    pub fn hst_listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.hst_listen)
    }

    pub fn sink_roll_time(&self) -> Duration {
        Duration::minutes(self.sink_roll_mins)
    }

    pub fn tick_interval(&self) -> Duration {
        Duration::seconds(self.tick_interval)
    }

    pub fn price_key(&self, token_type: BlockchainTokenTypeV1) -> Option<SolPubkey> {
        match token_type {
            BlockchainTokenTypeV1::Hnt => self
                .cluster
                .hnt_price_key
                .as_ref()
                .map(|key| SolPubkey::from_str(key).expect("unable to parse")),
            BlockchainTokenTypeV1::Hst => self
                .cluster
                .hst_price_key
                .as_ref()
                .map(|key| SolPubkey::from_str(key).expect("unable to parse")),
            BlockchainTokenTypeV1::Mobile => self
                .cluster
                .mobile_price_key
                .as_ref()
                .map(|key| SolPubkey::from_str(key).expect("unable to parse")),
            BlockchainTokenTypeV1::Iot => self
                .cluster
                .iot_price_key
                .as_ref()
                .map(|key| SolPubkey::from_str(key).expect("unable to parse")),
        }
    }
}
