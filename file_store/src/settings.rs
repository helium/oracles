use config::{Config, File};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::bucket_client::BucketClient;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    pub region: Option<String>,
    /// Optional api endpoint for the bucket. Default none
    pub endpoint: Option<String>,
    /// Should only be used for local testing
    #[serde(skip_serializing)]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct BucketSettings {
    bucket: String,
    #[serde(flatten)]
    settings: Settings,
}

impl Settings {
    /// Load Settings from a given path.
    ///
    /// Environment overrides are not supported for file_store cli commands
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, config::ConfigError> {
        Config::builder()
            .add_source(File::with_name(&path.as_ref().to_string_lossy()))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub async fn connect(&self) -> crate::Client {
        crate::new_client(
            self.region.clone(),
            self.endpoint.clone(),
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
        )
        .await
    }
}

impl BucketSettings {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, config::ConfigError> {
        Config::builder()
            .add_source(File::with_name(&path.as_ref().to_string_lossy()))
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub async fn connect(&self) -> BucketClient {
        BucketClient::new(
            self.bucket.clone(),
            self.settings.region.clone(),
            self.settings.endpoint.clone(),
            self.settings.access_key_id.clone(),
            self.settings.secret_access_key.clone(),
        )
        .await
    }
}
