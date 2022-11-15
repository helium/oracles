use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to "density_scaler=debug"
    #[serde(default = "default_log")]
    pub log: String,
    /// Follower settings
    pub follower: node_follower::Settings,
    /// Trigger every X minutes. Defaults to 30 mins.
    #[serde(default = "default_trigger_interval")]
    pub trigger: i64,
}

pub fn default_log() -> String {
    "density_scaler=debug".to_string()
}

fn default_trigger_interval() -> i64 {
    1800
}
