use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use helium_crypto::{Keypair, PublicKey};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path, str::FromStr, sync::Arc};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "mobile_config=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Listen address. Required. Default to 0.0.0.0::8080
    #[serde(default = "default_listen_addr")]
    pub listen: SocketAddr,
    /// Base64 encoded string with bytes of helium keypair
    #[serde(deserialize_with = "crate::deserialize_keypair", skip_serializing)]
    pub signing_keypair: Arc<Keypair>,
    /// B58 encoded public key of the default admin keypair
    pub admin_pubkey: PublicKey,
    /// Settings passed to the db_store crate for connecting to
    /// the config service's own persistence store
    pub database: db_store::Settings,
    /// Settings passed to the db_store crate for connecting to
    /// the database for Solana on-chain data
    pub metadata: db_store::Settings,
    #[serde(
        with = "humantime_serde",
        default = "default_mobile_radio_tracker_interval"
    )]
    pub mobile_radio_tracker_interval: std::time::Duration,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    #[serde(default = "default_boosted_hex_activation_cutoff")]
    pub boosted_hex_activation_cutoff: DateTime<Utc>,
}

fn default_boosted_hex_activation_cutoff() -> DateTime<Utc> {
    DateTime::from_str("2025-07-01T00:00:00Z").unwrap()
}

fn default_mobile_radio_tracker_interval() -> std::time::Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_log() -> String {
    "mobile_config=info".to_string()
}

fn default_listen_addr() -> SocketAddr {
    "0.0.0.0:6080".parse().unwrap()
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
            .add_source(
                Environment::with_prefix("MC")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
