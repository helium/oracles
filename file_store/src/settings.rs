use crate::{Error, Result};
use config::{Config, File};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    /// Bucket name for the store. Required
    pub bucket: String,
    /// Optional api endpoint for the bucket. Default none
    pub endpoint: Option<String>,
    /// Optional region for the endpoint. Default: us-west-2
    #[serde(default = "default_region")]
    pub region: String,

    /// Should only be used for local testing
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

pub fn default_region() -> String {
    "us-west-2".to_string()
}

impl Settings {
    /// Load Settings from a given path.
    ///
    /// Environment overrides are not supported for file_store cli commands
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Config::builder()
            .add_source(File::with_name(&path.as_ref().to_string_lossy()))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}
