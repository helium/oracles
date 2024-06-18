use anyhow::{anyhow, Result};
use config::{Config, Environment, File};
use helium_proto::BlockchainTokenTypeV1;
use humantime_serde::re::humantime;
use pyth_solana_receiver_sdk::price_update::{get_feed_id_from_hex, FeedId};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey as SolPubkey;
use std::{path::Path, str::FromStr, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub hnt_price_key: Option<String>,
    pub hnt_price_feed_id: Option<String>,
    pub hnt_price: Option<u64>,
    pub mobile_price_key: Option<String>,
    pub mobile_price_feed_id: Option<String>,
    pub mobile_price: Option<u64>,
    pub iot_price_key: Option<String>,
    pub iot_price_feed_id: Option<String>,
    pub iot_price: Option<u64>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: "devnet".to_string(),
            hnt_price_key: None,
            hnt_price_feed_id: None,
            hnt_price: None,
            mobile_price_key: None,
            mobile_price_feed_id: None,
            mobile_price: None,
            iot_price_key: None,
            iot_price_feed_id: None,
            iot_price: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
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
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    /// Cluster Configuration
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// How long to use a stale price in minutes
    #[serde(with = "humantime_serde", default = "default_stale_price_duration")]
    pub stale_price_duration: Duration,
    /// Interval when retrieving a pyth price from on chain
    #[serde(with = "humantime_serde", default = "default_pyth_price_interval")]
    pub pyth_price_interval: Duration,
}

fn default_pyth_price_interval() -> Duration {
    humantime::parse_duration("2 hours").unwrap()
}

fn default_source() -> String {
    "https://api.devnet.solana.com".to_string()
}

fn default_log() -> String {
    "price=debug".to_string()
}

fn default_interval() -> Duration {
    humantime::parse_duration("1 minute").unwrap()
}

fn default_stale_price_duration() -> Duration {
    humantime::parse_duration("12 hours").unwrap()
}

fn default_cache() -> String {
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

    pub fn price_key(&self, token_type: BlockchainTokenTypeV1) -> Result<Option<SolPubkey>> {
        self.key(token_type)?
            .as_ref()
            .map(|key| SolPubkey::from_str(key).map_err(|_| anyhow!("unable to parse {}", key)))
            .transpose()
    }

    pub fn price_feed_id(&self, token_type: BlockchainTokenTypeV1) -> Result<Option<FeedId>> {
        let feed_id = match token_type {
            BlockchainTokenTypeV1::Hnt => Ok(self.cluster.hnt_price_feed_id.as_deref()),
            BlockchainTokenTypeV1::Mobile => Ok(self.cluster.mobile_price_feed_id.as_deref()),
            BlockchainTokenTypeV1::Iot => Ok(self.cluster.iot_price_feed_id.as_deref()),
            _ => Err(anyhow::anyhow!("token type not supported")),
        }?;

        feed_id
            .map(|f| get_feed_id_from_hex(f).map_err(|_| anyhow::anyhow!("invalid feed id")))
            .transpose()
    }

    pub fn default_price(&self, token_type: BlockchainTokenTypeV1) -> Result<Option<u64>> {
        match token_type {
            BlockchainTokenTypeV1::Hnt => Ok(self.cluster.hnt_price),
            BlockchainTokenTypeV1::Iot => Ok(self.cluster.iot_price),
            BlockchainTokenTypeV1::Mobile => Ok(self.cluster.mobile_price),
            _ => Err(anyhow::anyhow!("token type not supported")),
        }
    }

    fn key(&self, token_type: BlockchainTokenTypeV1) -> Result<&Option<String>> {
        match token_type {
            BlockchainTokenTypeV1::Hnt => Ok(&self.cluster.hnt_price_key),
            BlockchainTokenTypeV1::Mobile => Ok(&self.cluster.mobile_price_key),
            BlockchainTokenTypeV1::Iot => Ok(&self.cluster.iot_price_key),
            _ => Err(anyhow::anyhow!("token type not supported")),
        }
    }
}
