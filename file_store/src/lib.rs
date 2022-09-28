pub mod cli;
mod error;
mod file_info;
pub mod file_sink;
pub mod file_source;
pub mod file_store;
pub mod file_upload;
pub mod heartbeat;
pub mod lora_beacon_report;
pub mod lora_valid_poc;
pub mod lora_witness_report;
pub mod speedtest;
pub mod traits;

pub use crate::file_store::FileStore;
pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};

use bytes::BytesMut;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::BoxStream;

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;

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

pub fn datetime_from_naive(v: NaiveDateTime) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(v, Utc)
}
