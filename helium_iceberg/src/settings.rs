use crate::catalog::Catalog;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub catalog_uri: String,
    pub catalog_name: String,
    pub warehouse: Option<String>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub s3: S3Config,
    /// Additional properties passed through to the catalog builder.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    #[serde(skip_serializing)]
    pub token: Option<String>,
    #[serde(skip_serializing)]
    pub credential: Option<String>,
    pub oauth2_server_uri: Option<String>,
    pub scope: Option<String>,
    pub audience: Option<String>,
    pub resource: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub endpoint: Option<String>,
    #[serde(skip_serializing)]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_access_key: Option<String>,
    pub region: Option<String>,
    pub path_style_access: Option<bool>,
    /// Skip loading credentials from environment variables and config files.
    /// Use this when you want to use explicit credentials instead of the default
    /// AWS credential chain.
    pub disable_config_load: Option<bool>,
}

/// Converts a list of key-value pairs into a `HashMap`, only inserting the key
/// when the value is `Some`.
macro_rules! option_hashmap {
    ( $($key:expr => $maybe_val:expr),+ $(,)? ) => {
        {
            let mut map = HashMap::new();
            $(
                if let Some(ref val) = $maybe_val {
                    map.insert($key.to_string(), val.to_string());
                }
            ) +
            map
        }
    };
}

impl S3Config {
    pub fn props(&self) -> HashMap<String, String> {
        option_hashmap! {
            "s3.endpoint" => self.endpoint,
            "s3.access-key-id" => self.access_key_id,
            "s3.secret-access-key" => self.secret_access_key,
            "s3.region" => self.region,
            "s3.path-style-access" => self.path_style_access,
            "s3.disable-config-load" => self.disable_config_load
        }
    }
}

impl AuthConfig {
    /// Produce the props HashMap entries expected by `iceberg-catalog-rest`.
    pub fn props(&self) -> HashMap<String, String> {
        option_hashmap! {
            "token" => self.token,
            "credential" => self.credential,
            "oauth2-server-uri" => self.oauth2_server_uri,
            "scope" => self.scope,
            "audience" => self.audience,
            "resource" => self.resource
        }
    }
}

impl Settings {
    /// Connect to an Iceberg REST catalog using these settings.
    ///
    /// This is a convenience method equivalent to `Catalog::connect(self)`.
    pub async fn connect(&self) -> Result<Catalog> {
        Catalog::connect(self).await
    }
}
