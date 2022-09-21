pub mod attach_event;
mod error;
mod event_id;
mod imsi;
pub mod server_5g;
pub mod server_lora;

pub use error::{Error, Result};
pub use event_id::EventId;
pub use imsi::Imsi;

use chrono::{DateTime, NaiveDateTime, Utc};

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}
