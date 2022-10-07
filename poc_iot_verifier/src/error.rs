use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
    #[error("custom error")]
    Custom(String),
    #[error("sql error")]
    Sql(#[from] sqlx::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("encode error")]
    Encode(#[from] EncodeError),
    #[error("decode error")]
    Decode(#[from] DecodeError),
    #[error("migration error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("service error")]
    Service(#[from] helium_proto::services::Error),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("store error")]
    Store(#[from] file_store::Error),
    #[error("not found")]
    NotFound(String),
    #[error("base64 decode error")]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("denylist error")]
    DenyList(#[from] denylist::Error),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("datetime error")]
    Chrono(#[from] chrono::ParseError),
    #[error("invalid decimals in {0}, only 8 allowed")]
    Decimals(String),
}

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::EncodeError),
    #[error("json error")]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn not_found<E: ToString>(msg: E) -> Self {
        Self::NotFound(msg.to_string())
    }
    pub fn custom<E: ToString>(msg: E) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl DecodeError {
    pub fn decimals(value: &str) -> Self {
        Self::Decimals(value.to_string())
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
from_err!(EncodeError, serde_json::Error);

// Decode Errors
from_err!(DecodeError, http::uri::InvalidUri);
from_err!(DecodeError, prost::DecodeError);
from_err!(DecodeError, chrono::ParseError);

impl From<Error> for (http::StatusCode, String) {
    fn from(v: Error) -> Self {
        match v {
            Error::NotFound(msg) => (http::StatusCode::NOT_FOUND, msg),
            err => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    }
}
