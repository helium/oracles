use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    /// Solana RpcClient URL:
    pub solana_rpc: String,
    pub dc_mint: Pubkey,
    pub dnt_mint: Pubkey,
    pub hnt_mint: Pubkey,
    pub dc_burn_authority: Pubkey,
    pub escrow_account: Pubkey,
    pub registrar: Pubkey,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    pub org: node_follower::Settings,
}

pub fn default_log() -> String {
    "iot_packet_verifier=debug,poc_store=info".to_string()
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new(path: Option<impl AsRef<Path>>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of VERIFY)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("PACKET_VERIFY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
