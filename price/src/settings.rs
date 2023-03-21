use anyhow::Result;
use chrono::Duration;
use config::{Config, Environment, File};
use helium_proto::BlockchainTokenTypeV1;
use serde::Deserialize;
use solana_program::pubkey::Pubkey as SolPubkey;
use std::{path::Path, str::FromStr};

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub hnt_price_key: Option<String>,
    pub hnt_price: Option<u64>,
    pub mobile_price_key: Option<String>,
    pub mobile_price: Option<u64>,
    pub iot_price_key: Option<String>,
    pub iot_price: Option<u64>,
    pub hst_price_key: Option<String>,
    pub hst_price: Option<u64>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: "devnet".to_string(),
            hnt_price_key: Some("6Eg8YdfFJQF2HHonzPUBSCCmyUEhrStg9VBLK957sBe6".to_string()),
            hnt_price: None,
            mobile_price_key: None,
            mobile_price: None,
            iot_price_key: None,
            iot_price: None,
            hst_price_key: None,
            hst_price: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    /// Source URL for price data. Required
    #[serde(default = "default_source")]
    pub source: String,
    /// Target output bucket details
    pub output: file_store::Settings,
    /// Folder for local cache of ingest data
    #[serde(default = "default_cache")]
    pub cache: String,
    /// Metrics settings
    pub metrics: poc_metrics::Settings,
    /// Tick interval (secs). Default = 60s.
    #[serde(default = "default_interval")]
    pub interval: i64,
    /// Price age (get price as long as it was updated within `age` seconds of current time) (in secs).
    /// Default = 60s.
    #[serde(default = "default_age")]
    pub age: u64,
    /// Cluster Configuration
    #[serde(default = "default_cluster")]
    pub cluster: ClusterConfig,
}

pub fn default_source() -> String {
    "https://api.devnet.solana.com".to_string()
}

pub fn default_log() -> String {
    "price=debug".to_string()
}

pub fn default_interval() -> i64 {
    60
}

pub fn default_age() -> u64 {
    60
}

pub fn default_cluster() -> ClusterConfig {
    ClusterConfig::default()
}

pub fn default_cache() -> String {
    "/var/data/price".to_string()
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

    pub fn interval(&self) -> Duration {
        Duration::seconds(self.interval)
    }

    pub fn price_key(&self, token_type: BlockchainTokenTypeV1) -> Option<SolPubkey> {
        match token_type {
            BlockchainTokenTypeV1::Hnt => self.cluster.hnt_price_key.as_ref().map(|key| {
                SolPubkey::from_str(key).unwrap_or_else(|_| panic!("unable to parse {}", key))
            }),
            BlockchainTokenTypeV1::Hst => self.cluster.hst_price_key.as_ref().map(|key| {
                SolPubkey::from_str(key).unwrap_or_else(|_| panic!("unable to parse {}", key))
            }),
            BlockchainTokenTypeV1::Mobile => self.cluster.mobile_price_key.as_ref().map(|key| {
                SolPubkey::from_str(key).unwrap_or_else(|_| panic!("unable to parse {}", key))
            }),
            BlockchainTokenTypeV1::Iot => self.cluster.iot_price_key.as_ref().map(|key| {
                SolPubkey::from_str(key).unwrap_or_else(|_| panic!("unable to parse {}", key))
            }),
        }
    }

    pub fn default_price(&self, token_type: BlockchainTokenTypeV1) -> Option<u64> {
        match token_type {
            BlockchainTokenTypeV1::Hnt => self.cluster.hnt_price,
            BlockchainTokenTypeV1::Iot => self.cluster.iot_price,
            BlockchainTokenTypeV1::Mobile => self.cluster.mobile_price,
            BlockchainTokenTypeV1::Hst => self.cluster.hst_price,
        }
    }
}
