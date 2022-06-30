pub mod api;
mod attach_event;
pub mod cli;
mod error;
mod follower;
mod gateway;
mod heartbeat;
mod imsi;
mod maker;
mod public_key;
mod speedtest;
mod uuid;

pub use attach_event::CellAttachEvent;
pub use error::{Error, Result};
pub use follower::Follower;
pub use gateway::Gateway;
pub use heartbeat::CellHeartbeat;
pub use imsi::Imsi;
pub use maker::Maker;
pub use public_key::PublicKey;
pub use speedtest::CellSpeedtest;
pub use uuid::Uuid;

use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Since {
    pub since: Option<DateTime<Utc>>,
    pub count: Option<usize>,
}

use chrono::NaiveDateTime;
pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}
