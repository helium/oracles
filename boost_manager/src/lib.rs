use serde::{Deserialize, Serialize};

pub mod activator;
mod db;
pub mod settings;
pub mod telemetry;
pub use settings::Settings;
pub mod updater;
pub mod watcher;

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Deserialize, Serialize, sqlx::Type)]
#[sqlx(type_name = "onchain_status")]
#[sqlx(rename_all = "lowercase")]
pub enum OnChainStatus {
    Queued = 0,
    Pending = 1,
    Success = 2,
    Failed = 3,
    Cancelled = 4,
}
