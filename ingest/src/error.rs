use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
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
    Store(#[from] poc_store::Error),
    #[error("env error")]
    Env(#[from] std::env::VarError),
    #[error("not found")]
    NotFound(String),
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
