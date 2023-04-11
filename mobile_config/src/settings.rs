use config::{Config, Environment, File};
use serde::Deserialize;
use std::{
    net::{AddrParseError, SocketAddr},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "mobile_config=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Listen address. Required. Default to 0.0.0.0::8080
    #[serde(default = "default_listen_addr")]
    pub listen: String,
    /// File from which to load config server signing keypair
    pub signing_keypair: String,
    /// B58 encoded public key of the default admin keypair
    pub admin_pubkey: String,
    /// Settings passed to the db_store crate for connecting to
    /// the config service's own persistence store
    pub database: db_store::Settings,
    /// Settings passed to the db_store crate for connecting to
    /// the database for Solana on-chain data
    pub metadata: db_store::Settings,
    pub metrics: poc_metrics::Settings,
}

pub fn default_log() -> String {
    "mobile_config=debug".to_string()
}

pub fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

impl Settings {
    /// Settings can be loaded from a given optional path and
    /// can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries
    /// in the settings file in uppercase and prefixed with "CFG_".
    /// Example: "CFG_DATABASE_URL" will override the database url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }

        // Add in settings from the environment (with prefix of APP)
        // E.g. `CFG_DEBUG=1 .target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("CFG").separator("__"))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn listen_addr(&self) -> Result<SocketAddr, AddrParseError> {
        SocketAddr::from_str(&self.listen)
    }

    pub fn signing_keypair(&self) -> anyhow::Result<helium_crypto::Keypair> {
        let data = std::fs::read(&self.signing_keypair).map_err(helium_crypto::Error::from)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }

    pub fn admin_pubkey(&self) -> anyhow::Result<helium_crypto::PublicKey> {
        Ok(helium_crypto::PublicKey::from_str(&self.admin_pubkey)?)
    }
}
