pub mod api;
mod cell_type;
pub mod cli;
mod error;
mod event_id;
mod file_writer;
mod follower;
mod imsi;
mod maker;
mod public_key;
pub mod rewards;
mod uuid;

pub use cell_type::CellType;
pub use error::{Error, Result};
pub use event_id::EventId;
pub use file_writer::FileWriter;
pub use follower::Follower;
pub use imsi::Imsi;
pub use maker::Maker;
pub use public_key::PublicKey;
pub use uuid::Uuid;

use chrono::{DateTime, NaiveDateTime, Utc};

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}
