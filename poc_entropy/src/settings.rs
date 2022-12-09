use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "poc_entropy=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Listen address for http requests for entropy. Default "0.0.0.0:8080"
    #[serde(default = "default_listen_addr")]
    pub listen: String,
    /// Source URL for entropy data. Required
    pub source: String,
    /// Target output bucket details
    pub output: file_store::Settings,
    /// Folder for locacl cache of ingest data
    #[serde(default = "default_cache")]
    pub cache: String,
    /// Metrics settings
    pub metrics: poc_metrics::Settings,
}

pub fn default_log() -> String {
    "poc_entropy=debug,poc_store=info".to_string()
}

pub fn default_cache() -> String {
    "/var/data/entropy".to_string()
}

pub fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
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
            .add_source(Environment::with_prefix("ENTROPY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
