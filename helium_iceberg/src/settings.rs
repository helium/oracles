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
}

impl S3Config {
    pub fn props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(ref endpoint) = self.endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(ref access_key_id) = self.access_key_id {
            props.insert("s3.access-key-id".to_string(), access_key_id.clone());
        }
        if let Some(ref secret_access_key) = self.secret_access_key {
            props.insert("s3.secret-access-key".to_string(), secret_access_key.clone());
        }
        if let Some(ref region) = self.region {
            props.insert("s3.region".to_string(), region.clone());
        }
        if let Some(path_style_access) = self.path_style_access {
            props.insert(
                "s3.path-style-access".to_string(),
                path_style_access.to_string(),
            );
        }
        props
    }
}

impl AuthConfig {
    /// Produce the props HashMap entries expected by `iceberg-catalog-rest`.
    pub fn props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(ref token) = self.token {
            props.insert("token".to_string(), token.clone());
        }
        if let Some(ref credential) = self.credential {
            props.insert("credential".to_string(), credential.clone());
        }
        if let Some(ref uri) = self.oauth2_server_uri {
            props.insert("oauth2-server-uri".to_string(), uri.clone());
        }
        if let Some(ref scope) = self.scope {
            props.insert("scope".to_string(), scope.clone());
        }
        if let Some(ref audience) = self.audience {
            props.insert("audience".to_string(), audience.clone());
        }
        if let Some(ref resource) = self.resource {
            props.insert("resource".to_string(), resource.clone());
        }
        props
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
