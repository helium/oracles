use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use helium_crypto::{Keypair, PublicKey};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path, sync::Arc};

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
    #[serde(
        deserialize_with = "crate::deserialize_helium_keypair",
        skip_serializing
    )]
    pub signing_keypair: Arc<Keypair>,
    /// B58 encoded public key of the default admin keypair
    pub admin_pubkey: PublicKey,
    /// Settings passed to the db_store crate for connecting to
    /// the config service's own persistence store
    pub database: db_store::Settings,
    /// Settings passed to the db_store crate for connecting to
    /// the database for Solana on-chain data
    pub metadata: db_store::Settings,
    /// S3 bucket holding chain_rewardable_entities change reports
    /// (mobile_hotspot_change_report and entity_ownership_change_report).
    pub ingest: file_store::BucketSettings,
    /// Cold-start timestamp for both change-report pollers. Once
    /// `files_processed` is populated, that table drives the offset
    /// and this field is only consulted on a fresh DB.
    #[serde(default = "default_gateway_stream_start_after")]
    pub gateway_stream_start_after: DateTime<Utc>,
    #[serde(with = "humantime_serde", default = "default_gateway_tracker_interval")]
    pub gateway_tracker_interval: std::time::Duration,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
}

fn default_gateway_tracker_interval() -> std::time::Duration {
    // Tracker runs as a daily reconciliation pass; the per-event S3 change
    // streams in mobile_config/src/gateway/{hotspot,ownership}_change_stream.rs
    // are the primary update path.
    humantime::parse_duration("1 day").unwrap()
}

fn default_gateway_stream_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
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
