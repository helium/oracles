pub mod cli;
pub mod entropy_report;
mod error;
mod file_info;
pub mod file_info_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_store;
pub mod file_upload;
pub mod heartbeat;
pub mod iot_beacon_report;
pub mod iot_invalid_poc;
pub mod iot_packet;
pub mod iot_valid_poc;
pub mod iot_witness_report;
pub mod mobile_session;
pub mod mobile_subscriber;
pub mod mobile_transfer;
pub mod reward_manifest;
mod settings;
pub mod speedtest;
pub mod traits;

pub use crate::file_store::FileStore;
pub use cli::bucket::FileFilter;
pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};
pub use iot_valid_poc::SCALING_PRECISION;
pub use settings::Settings;

use bytes::BytesMut;
use futures::stream::BoxStream;

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;
