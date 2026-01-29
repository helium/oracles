use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    file_info::FileInfoError, gzipped_framed_file::GzippedFramedFileError,
    rolling_file_sink::RollingFileSinkError,
};

pub use aws_error::AwsError;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("prost encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("file info error: {0}")]
    FileInfo(#[from] FileInfoError),

    #[error("aws error: {0}")]
    Aws(#[from] Box<AwsError>),

    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("error building file info poller: {0}")]
    FileInfoPollerError(#[from] crate::file_info_poller::FileInfoPollerConfigBuilderError),

    #[cfg(feature = "sqlx-postgres")]
    #[error("db error")]
    DbError(#[from] sqlx::Error),

    #[error("error write data to file on disk: {0}")]
    FileWriteError(#[from] GzippedFramedFileError),

    #[error("rolling file sink error: {0}")]
    RollingFileSink(#[from] RollingFileSinkError),

    // Generic error wrapper for external (out of that repository) traits implementations.
    // Not recommended for internal use!
    #[error("external error")]
    ExternalError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(thiserror::Error, Debug)]
pub enum ChannelError {
    #[error("failed to send {prefix} for process {process}")]
    PollerSendError { prefix: String, process: String },

    #[error("channel closed sink {name}")]
    SinkClosed { name: String },

    #[error("timeout for sink {name}")]
    SinkTimeout { name: String },

    #[error("channel closed for upload {path}")]
    UploadClosed { path: PathBuf },
}

impl ChannelError {
    pub fn poller_send_error(prefix: &str, process: &str) -> Error {
        Error::Channel(Self::PollerSendError {
            prefix: prefix.to_owned(),
            process: process.to_owned(),
        })
    }

    pub fn sink_closed(name: &str) -> Error {
        Error::Channel(Self::SinkClosed {
            name: name.to_owned(),
        })
    }

    pub fn sink_timeout(name: &str) -> Error {
        Error::Channel(Self::SinkTimeout {
            name: name.to_owned(),
        })
    }

    pub fn upload_closed(path: &Path) -> Error {
        Error::Channel(Self::UploadClosed {
            path: path.to_owned(),
        })
    }
}

mod aws_error {
    use std::path::PathBuf;

    use super::Error;

    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::{
        operation::{
            delete_object::DeleteObjectError, get_object::GetObjectError,
            list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
        },
        primitives::ByteStreamError,
    };

    #[derive(thiserror::Error, Debug)]
    pub enum AwsError {
        #[error("put_object ({bucket}::{file_path}): {source}")]
        PutObject {
            source: PutObjectError,
            bucket: String,
            file_path: PathBuf,
        },

        #[error("pub_object_byte_stream ({bucket}::{file_path}): {source}")]
        PubObjectByteStream {
            source: ByteStreamError,
            bucket: String,
            file_path: PathBuf,
        },

        #[error("delete_object ({bucket}::{key}): {source}")]
        DeleteObject {
            source: DeleteObjectError,
            bucket: String,
            key: String,
        },

        #[error("get_object ({bucket}::{key}): {source}")]
        GetObject {
            source: GetObjectError,
            bucket: String,
            key: String,
        },

        #[error("list_object ({bucket}::{prefix}): {source}")]
        ListObject {
            source: ListObjectsV2Error,
            bucket: String,
            prefix: String,
        },
    }

    impl AwsError {
        pub fn put_object_error(
            err: SdkError<PutObjectError>,
            bucket: &str,
            file_path: impl Into<PathBuf>,
        ) -> Error {
            Error::Aws(Box::new(Self::PutObject {
                source: err.into_service_error(),
                bucket: bucket.into(),
                file_path: file_path.into(),
            }))
        }

        pub fn pub_object_byte_stream_error(
            err: ByteStreamError,
            bucket: impl Into<String>,
            file_path: impl Into<PathBuf>,
        ) -> Error {
            Error::Aws(Box::new(Self::PubObjectByteStream {
                source: err,
                bucket: bucket.into(),
                file_path: file_path.into(),
            }))
        }

        pub fn delete_object_error(
            err: SdkError<DeleteObjectError>,
            bucket: impl Into<String>,
            key: impl Into<String>,
        ) -> Error {
            Error::Aws(Box::new(Self::DeleteObject {
                source: err.into_service_error(),
                bucket: bucket.into(),
                key: key.into(),
            }))
        }

        pub fn get_object_error(
            err: SdkError<GetObjectError>,
            bucket: impl Into<String>,
            key: impl Into<String>,
        ) -> Error {
            Error::Aws(Box::new(Self::GetObject {
                source: err.into_service_error(),
                bucket: bucket.into(),
                key: key.into(),
            }))
        }

        pub fn list_object_error(
            err: SdkError<ListObjectsV2Error>,
            bucket: &str,
            prefix: &str,
        ) -> Error {
            Error::Aws(Box::new(Self::ListObject {
                source: err.into_service_error(),
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
            }))
        }
    }
}
