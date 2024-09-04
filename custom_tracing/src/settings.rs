use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    /// File name to be watched by custom tracing
    #[serde(default = "default_tracing_cfg_file")]
    pub tracing_cfg_file: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tracing_cfg_file: default_tracing_cfg_file(),
        }
    }
}

pub fn default_tracing_cfg_file() -> String {
    "tracing.cfg".to_string()
}
