use anyhow::Result;
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use serde_json;
use solana::Token;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct TokenSetting {
    pub token: Token,
    pub default_price: Option<u64>,
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
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    /// Tick interval (secs). Default = 60s.
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    pub tokens: Vec<String>,
    /// How long to use a stale price in minutes
    #[serde(with = "humantime_serde", default = "default_stale_price_duration")]
    pub stale_price_duration: Duration,
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
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
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
            .add_source(Environment::with_prefix("PRICE").separator("__").list_separator(",").with_list_parse_key("tokens").try_parsing(true))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn tokens(&self) -> anyhow::Result<Vec<TokenSetting>> {
        let mut token_settings = Vec::new();
        let mut errors = Vec::new();
        
        for (i, token_json_str) in self.tokens.iter().enumerate() {
            match serde_json::from_str::<TokenSetting>(token_json_str) {
                Ok(token_setting) => token_settings.push(token_setting),
                Err(e) => {
                    errors.push(format!("Token {} at index {}: {}", token_json_str, i, e));
                }
            }
        }
        
        if !errors.is_empty() {
            anyhow::bail!("Failed to parse some tokens:\n{}", errors.join("\n"));
        }
        
        Ok(token_settings)
    }
}
