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
