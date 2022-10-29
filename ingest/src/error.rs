use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("config error")]
    Config(#[from] config::ConfigError),
    #[error("metrics error")]
    Metrics(#[from] poc_metrics::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("encode error")]
    Encode(#[from] EncodeError),
    #[error("dencode error")]
    Decode(#[from] DecodeError),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("service error")]
    Service(#[from] helium_proto::services::Error),
    #[error("store error")]
    Store(#[from] file_store::Error),
    #[error("not found")]
    NotFound(String),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("socket addr error")]
    SocketAddr(#[from] std::net::AddrParseError),
}

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::EncodeError),
}

impl Error {
    pub fn not_found<E: ToString>(msg: E) -> Self {
        Self::NotFound(msg.to_string())
    }
}

impl From<Error> for tonic::Status {
    fn from(v: Error) -> Self {
        match v {
            Error::NotFound(msg) => tonic::Status::not_found(msg),
            _other => tonic::Status::internal("internal error"),
        }
    }
}

impl From<Error> for (http::StatusCode, String) {
    fn from(v: Error) -> Self {
        (http::StatusCode::INTERNAL_SERVER_ERROR, v.to_string())
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

// Encode Errors
from_err!(EncodeError, prost::EncodeError);

// Decode Errors
from_err!(DecodeError, http::uri::InvalidUri);
from_err!(DecodeError, prost::DecodeError);
