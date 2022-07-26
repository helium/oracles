pub mod cli;
mod error;
mod file_info;
pub mod file_sink;
pub mod file_source;
pub mod heartbeat;
pub mod public_key;
pub mod speedtest;

pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};
pub use file_source::FileSource;
pub use public_key::PublicKey;

use chrono::{DateTime, NaiveDateTime, Utc};

pub fn datetime_from_epoch(secs: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs as i64, 0), Utc)
}

pub fn datetime_from_epoch_millis(millis: u64) -> DateTime<Utc> {
    let secs = millis / 1000;
    let nsecs = (millis % 1000) * 1_000_000;
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(secs as i64, nsecs as u32),
        Utc,
    )
}
