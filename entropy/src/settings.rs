use crate::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string
    pub log: Option<String>,
    /// Target output bucket details
    pub output: file_store::Settings,
    /// Metrics settings
    pub metrics: poc_metrics::Settings,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "ENTROPY_". For example
    /// "ENTROPY_LOG" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("ENTROPY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}
