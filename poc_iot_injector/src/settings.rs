use crate::{Error, Result};
use config::{Config, Environment, File};
use file_store::file_sink;
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "poc_iot_injector=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// File to load keypair from
    pub keypair: String,
    /// Trigger interval in seconds. (Default is 1800; 30 minutes)
    #[serde(default = "default_trigger_interval")]
    pub trigger: u64,
    /// Last PoC submission timestamp in seconds since unix epoch. (Default is
    /// unix epoch)
    #[serde(default = "default_last_poc_submission")]
    pub last_poc_submission: i64,
    pub database: db_store::Settings,
    pub transactions: node_follower::Settings,
    pub verifier: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    /// Local folder for storing intermediate files
    pub cache: String,
    pub output: file_store::Settings,
}

pub fn default_log() -> String {
    "poc_iot_injector=debug,poc_store=info".to_string()
}

pub fn default_last_poc_submission() -> i64 {
    0
}

fn default_trigger_interval() -> u64 {
    1800
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "INJECT_". For example
    /// "INJECT_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of INJECT)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("INJECT").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }

    pub fn keypair(&self) -> Result<helium_crypto::Keypair> {
        let data = std::fs::read(&self.keypair)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }

    pub fn trigger_interval(&self) -> Duration {
        Duration::from_secs(self.trigger)
    }

    pub fn receipt_sender_receiver(
        &self,
    ) -> (file_sink::MessageSender, file_sink::MessageReceiver) {
        file_sink::message_channel(50)
    }
}
