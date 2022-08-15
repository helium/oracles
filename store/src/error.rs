use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("encode error")]
    Encode(#[from] EncodeError),
    #[error("dencode error")]
    Decode(#[from] DecodeError),
    #[error("not found")]
    NotFound(String),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("csv error")]
    Csv(#[from] csv::Error),
    #[error("aws error")]
    Aws(#[from] aws_sdk_s3::Error),
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
    #[error("env var error")]
    Env(#[from] std::env::VarError),
    #[error("mpsc channel error")]
    Channel,
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("file info error")]
    FileInfo(String),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
}

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::EncodeError),
    #[error("json error")]
    Json(#[from] serde_json::Error),
}

macro_rules! from_err {
    ($to_type:ty, $from_type:ty) => {
        impl From<$from_type> for Error {
            fn from(v: $from_type) -> Self {
                Self::from(<$to_type>::from(v))
            }
        }
    };
}

// Encode Errors
from_err!(EncodeError, prost::EncodeError);
from_err!(EncodeError, serde_json::Error);

// Decode Errors
from_err!(DecodeError, prost::DecodeError);

impl Error {
    pub fn not_found<E: ToString>(msg: E) -> Self {
        Self::NotFound(msg.to_string())
    }

    pub fn channel() -> Error {
        Error::Channel
    }

    pub fn s3_error<T>(err: T) -> Self
    where
        T: Into<aws_sdk_s3::Error>,
    {
        Self::from(err.into())
    }
}

impl DecodeError {
    pub fn file_info<E: ToString>(msg: E) -> Error {
        Error::Decode(Self::FileInfo(msg.to_string()))
    }
}
