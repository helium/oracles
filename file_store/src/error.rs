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
    Crypto(Box<helium_crypto::Error>),
    #[error("csv error")]
    Csv(#[from] csv::Error),
    #[error("aws error")]
    Aws(#[from] aws_sdk_s3::Error),
    #[error("config error")]
    Config(#[from] config::ConfigError),
    #[error("mpsc channel error")]
    Channel,
    #[error("no manifest")]
    NoManifest,
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("file info error")]
    FileInfo(String),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("integer conversion error")]
    FromInt(#[from] std::num::TryFromIntError),
    #[error("unsupported datarate, type: {0}, value: {1}")]
    UnsupportedDataRate(String, i32),
    #[error("unsupported invalid_reason, type: {0}, value: {1}")]
    UnsupportedInvalidReason(String, i32),
    #[error("unsupported participant_side, type: {0}, value: {1}")]
    UnsupportedParticipantSide(String, i32),
    #[error("unsupported verification status, type: {0}, value: {1}")]
    UnsupportedStatusReason(String, i32),
    #[error("invalid unix timestamp {0}")]
    InvalidTimestamp(u64),
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

    pub fn unsupported_datarate<E: ToString>(msg1: E, msg2: i32) -> Error {
        Error::Decode(Self::UnsupportedDataRate(msg1.to_string(), msg2))
    }

    pub fn unsupported_participant_side<E: ToString>(msg1: E, msg2: i32) -> Error {
        Error::Decode(Self::UnsupportedParticipantSide(msg1.to_string(), msg2))
    }

    pub fn unsupported_invalid_reason<E: ToString>(msg1: E, msg2: i32) -> Error {
        Error::Decode(Self::UnsupportedInvalidReason(msg1.to_string(), msg2))
    }

    pub fn invalid_timestamp(v: u64) -> Error {
        Error::Decode(Self::InvalidTimestamp(v))
    }

    pub fn unsupported_status_reason<E: ToString>(msg1: E, msg2: i32) -> Error {
        Error::Decode(Self::UnsupportedInvalidReason(msg1.to_string(), msg2))
    }
}

impl From<helium_crypto::Error> for Error {
    fn from(err: helium_crypto::Error) -> Self {
        Self::Crypto(Box::new(err))
    }
}
