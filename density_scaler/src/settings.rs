use crate::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to "density_scaler=debug"
    #[serde(default = "default_log")]
    pub log: String,
    /// Follower settings
    pub follower: node_follower::Settings,
    /// Trigger every X minutes. Defaults to 30 mins.
    #[serde(default = "default_trigger_interval")]
    pub trigger: i64,
}

pub fn default_log() -> String {
    "density_scaler=debug".to_string()
}

fn default_trigger_interval() -> i64 {
    1800
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "DENSITY_SCALER". For example
    /// "DENSITY_SCALER_TRIGGER_INTERVAL" will override the trigger_interval
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of DENSITY_SCALER)
        // Eg.. `DENSITY_SCALER_ DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("DENSITY_SCALER").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}
