use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub catalog_uri: String,
    pub catalog_name: String,
    pub warehouse: Option<String>,
    #[serde(skip_serializing)]
    pub auth_token: Option<String>,
}
