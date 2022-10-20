use crate::{Error, Result};
use config::{Config, Environment, File};
use http::Uri;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::{path::Path, time};

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string
    pub log: Option<String>,
    /// Check interval in seconds. (Default is 900; 15 minutes)
    #[serde(default = "default_interval")]
    pub interval: u64,
    pub database: Database,
    pub verifier: file_store::FileStoreSettings,
    pub output: file_store::FileStoreSettings,
    pub metrics: poc_metrics::MetricsSettings,
}

#[derive(Debug, Deserialize)]
pub struct Database {
    /// Max open connections to the database. (Default 10)
    #[serde(default = "default_db_connections")]
    pub max_connections: u32,
    /// URL to access the postgres database. For example:
    /// =postgres://postgres:postgres@127.0.0.1:5432/mobile_index_db
    #[serde(with = "http_serde::uri")]
    pub url: Uri,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overriden with environment variables.
    ///
    /// Environemnt overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "MI_". For example "MI_DATABASE_URL"
    /// will override the key file location.
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
            .add_source(Environment::with_prefix("MI").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }

    pub fn interval(&self) -> time::Duration {
        time::Duration::from_secs(self.interval)
    }
}

fn default_db_connections() -> u32 {
    10
}

fn default_interval() -> u64 {
    900
}

impl Database {
    pub async fn connect(&self) -> Result<Pool<Postgres>> {
        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect(&self.url.to_string())
            .await?;
        Ok(pool)
    }
}
