use crate::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "denylist=debug"
    #[serde(default = "default_log")]
    pub log: String,
    /// Listen address for http requests for entropy. Default "0.0.0.0:8080"
    #[serde(default = "default_denylist_url")]
    pub denylist_url: String,
    /// Cadence at which we poll for an updated denylist (secs)
    #[serde(default = "default_trigger_interval")]
    pub trigger: u64,
}

pub fn default_log() -> String {
    "denylist=debug".to_string()
}

pub fn default_denylist_url() -> String {
    "https://api.github.com/repos/helium/denylist/releases/latest".to_string()
}

fn default_trigger_interval() -> u64 {
    21600
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "DENYLIST_". For example
    /// "DENYLIST_LOG" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `DENYLIST_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("DENYLIST").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }

    pub fn trigger_interval(&self) -> Duration {
        Duration::from_secs(self.trigger)
    }
}
