use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("config error")]
    Config(#[from] config::ConfigError),
    #[error("custom error")]
    Custom(String),
    #[error("decode error")]
    Decode(#[from] DecodeError),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("metrics error")]
    Metrics(#[from] poc_metrics::Error),
    #[error("not found")]
    NotFound(String),
    #[error("service error")]
    Service(#[from] helium_proto::services::Error),
    #[error("store error")]
    Store(#[from] file_store::Error),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    /* DELETE ME...
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("datetime error")]
    Chrono(#[from] chrono::ParseError),
    #[error("invalid decimals in {0}, only 8 allowed")]
    Decimals(String),
    */
}

impl Error {
    pub fn not_found<E: ToString>(msg: E) -> Self {
        Self::NotFound(msg.to_string())
    }
    pub fn custom<E: ToString>(msg: E) -> Self {
        Self::Custom(msg.to_string())
    }
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

// Decode Errors
from_err!(DecodeError, prost::DecodeError);
