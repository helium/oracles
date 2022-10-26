use crate::{Error, Result};
use config::{Config, Environment, File};
use http::Uri;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    /// Cache location for generated verified reports
    pub cache: String,
    pub database: Database,
    pub follower: node_follower::Settings,
    pub ingest: file_store::Settings,
    pub entropy: file_store::Settings,
    pub output: file_store::Settings,
    pub metrics: poc_metrics::Settings,
}

pub fn default_log() -> String {
    "poc_iot_verifier=debug,poc_store=info".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct Database {
    /// Max open connections to the database. If absent a default is calculated
    /// by application code
    pub max_connections: Option<u32>,
    /// URL to access the postgres database. For example:
    /// postgres://postgres:postgres@127.0.0.1:5432/mobile_index_db
    #[serde(with = "http_serde::uri")]
    pub url: Uri,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of VERIFY)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(Environment::with_prefix("VERIFY").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}

impl Database {
    pub async fn connect(&self, default_max_connections: usize) -> Result<Pool<Postgres>> {
        let pool = PgPoolOptions::new()
            .max_connections(
                self.max_connections
                    .unwrap_or(default_max_connections as u32),
            )
            .connect(&self.url.to_string())
            .await?;
        Ok(pool)
    }
}
