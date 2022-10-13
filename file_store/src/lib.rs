pub mod cli;
mod error;
mod file_info;
pub mod file_sink;
pub mod file_source;
pub mod file_store;
pub mod file_upload;
pub mod heartbeat;
pub mod lora_beacon_report;
pub mod lora_invalid_poc;
pub mod lora_valid_poc;
pub mod lora_witness_report;
pub mod speedtest;
pub mod traits;

pub use crate::file_store::FileStore;
pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};

use bytes::BytesMut;
use futures::stream::BoxStream;

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;
