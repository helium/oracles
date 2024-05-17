use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    /// File name to be watched by custom tracing
    #[serde(default = "default_tracing_cfg_file")]
    pub tracing_cfg_file: String,
}

pub fn default_tracing_cfg_file() -> String {
    "tracing.cfg".to_string()
}
