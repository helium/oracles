use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{file_info::FileInfoError, gzipped_framed_file::GzippedFramedFileError};

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
    Aws(#[from] AwsError),

    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("error building file info poller: {0}")]
    FileInfoPollerError(#[from] crate::file_info_poller::FileInfoPollerConfigBuilderError),

    #[cfg(feature = "sqlx-postgres")]
    #[error("db error")]
    DbError(#[from] sqlx::Error),

    #[error("error write data to file on disk: {0}")]
    FileWriteError(#[from] GzippedFramedFileError),

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
        #[error("put_object: {0}")]
        PutObject(PutObjectError),

        #[error("pub_object_byte_stream: {0}")]
        PubObjectByteStream(ByteStreamError),

        #[error("delete_object: {0}")]
        DeleteObject(DeleteObjectError),

        #[error("get_object: {0}")]
        GetObject(GetObjectError),

        #[error("list_object: {0}")]
        ListObject(ListObjectsV2Error),
    }

    impl AwsError {
        pub fn put_object_error(err: SdkError<PutObjectError>) -> Error {
            Error::Aws(Self::PutObject(err.into_service_error()))
        }

        pub fn pub_object_byte_stream_error(err: ByteStreamError) -> Error {
            Error::Aws(Self::PubObjectByteStream(err))
        }

        pub fn delete_object_error(err: SdkError<DeleteObjectError>) -> Error {
            Error::Aws(Self::DeleteObject(err.into_service_error()))
        }

        pub fn get_object_error(err: SdkError<GetObjectError>) -> Error {
            Error::Aws(Self::GetObject(err.into_service_error()))
        }

        pub fn list_object_error(err: SdkError<ListObjectsV2Error>) -> Error {
            Error::Aws(Self::ListObject(err.into_service_error()))
        }
    }
}
