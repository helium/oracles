use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::file_info::FileInfoError;

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

    #[error("crypto error: {0}")]
    Crypto(Box<helium_crypto::Error>),

    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("error building file info poller: {0}")]
    FileInfoPollerError(#[from] crate::file_info_poller::FileInfoPollerConfigBuilderError),

    #[cfg(feature = "sqlx-postgres")]
    #[error("db error")]
    DbError(#[from] sqlx::Error),

    // Generic error wrapper for external (out of that repository) traits implementations.
    // Not recommended for internal use!
    #[error("external error")]
    ExternalError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(thiserror::Error, Debug)]
pub enum AwsError {
    #[error("s3: {0}")]
    S3(Box<aws_sdk_s3::Error>),

    #[error("streaming: {0}")]
    Streaming(#[from] aws_sdk_s3::primitives::ByteStreamError),
}

impl AwsError {
    pub fn s3_error<T>(err: T) -> Error
    where
        T: Into<aws_sdk_s3::Error>,
    {
        Error::from(err.into())
    }
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

impl From<helium_crypto::Error> for Error {
    fn from(err: helium_crypto::Error) -> Self {
        Self::Crypto(Box::new(err))
    }
}

impl From<aws_sdk_s3::Error> for Error {
    fn from(err: aws_sdk_s3::Error) -> Self {
        Self::Aws(AwsError::S3(Box::new(err)))
    }
}

impl From<aws_sdk_s3::primitives::ByteStreamError> for Error {
    fn from(err: aws_sdk_s3::primitives::ByteStreamError) -> Self {
        Self::from(AwsError::Streaming(err))
    }
}
