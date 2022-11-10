use crate::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_rewards=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// File to load keypair from
    pub keypair: String,
    /// Trigger interval in seconds. (Default is 900; 15 minutes)
    #[serde(default = "default_trigger_interval")]
    pub trigger: i64,
    /// Rewards interval in seconds. (Default is 86400; 24 hours)
    #[serde(default = "default_rewards_interval")]
    pub rewards: i64,
    pub database: db_store::Settings,
    pub follower: node_follower::Settings,
    pub transactions: node_follower::Settings,
    pub verifier: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
}

pub fn default_log() -> String {
    "mobile_rewards=debug,poc_store=info".to_string()
}

fn default_trigger_interval() -> i64 {
    900
}

fn default_rewards_interval() -> i64 {
    86400
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "MI_". For example "MI_DATABASE_URL"
    /// will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of MI)
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("MR").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }

    pub fn keypair(&self) -> Result<helium_crypto::Keypair> {
        let data = std::fs::read(&self.keypair)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }
}
