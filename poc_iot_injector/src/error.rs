use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("store error")]
    Store(#[from] file_store::Error),
    #[error("sql error")]
    Sql(#[from] sqlx::Error),
    #[error("env error")]
    Env(#[from] std::env::VarError),
    #[error("encode error")]
    Encode(#[from] EncodeError),
    #[error("decode error")]
    Decode(#[from] DecodeError),
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("env not found: {0}")]
    EnvNotFound(String),
    #[error("migration error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("zero witnesses error")]
    ZeroWitnesses,
    #[error("invalid exponent {0} error")]
    InvalidExponent(String),
    #[error("meta error")]
    MetaError(#[from] db_store::MetaError),
    #[error("follower error")]
    FollowerError(#[from] node_follower::Error),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
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
from_err!(DecodeError, http::uri::InvalidUri);
from_err!(DecodeError, prost::DecodeError);
