use config::{Config, ConfigError, Environment, File};
use helium_proto::services::{iot_config::config_org_client::OrgClient, Channel, Endpoint};
use serde::Deserialize;
use solana_sdk::pubkey::{ParsePubkeyError, Pubkey};
use std::path::{Path, PathBuf};

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
    /// Path to the keypair for signing burn transactions
    pub burn_keypair: PathBuf,
    /// Path to the keypair for signing config changes
    pub config_keypair: PathBuf,
    /// Data credit burn period in minutes. Default is 1.
    pub burn_period: u64,
    pub cluster: String,
    pub dc_mint: String,
    pub dnt_mint: String,
    pub hnt_mint: String,
    pub database: db_store::Settings,
    pub ingest: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
    #[serde(with = "http_serde::uri", default = "default_url")]
    pub org_url: http::Uri,
    #[serde(default)]
    pub enable_dc_burn: bool,
}

pub fn default_burn_period() -> u64 {
    1
}

pub fn default_url() -> http::Uri {
    http::Uri::from_static("http://127.0.0.1:8080")
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

    pub fn dc_mint(&self) -> Result<Pubkey, ParsePubkeyError> {
        self.dc_mint.parse()
    }

    pub fn dnt_mint(&self) -> Result<Pubkey, ParsePubkeyError> {
        self.dnt_mint.parse()
    }

    pub fn hnt_mint(&self) -> Result<Pubkey, ParsePubkeyError> {
        self.hnt_mint.parse()
    }

    pub fn connect_org(&self) -> OrgClient<Channel> {
        OrgClient::new(Endpoint::from(self.org_url.clone()).connect_lazy())
    }

    pub fn config_keypair(&self) -> Result<helium_crypto::Keypair, Box<helium_crypto::Error>> {
        let data = std::fs::read(&self.config_keypair).map_err(helium_crypto::Error::from)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }
}
