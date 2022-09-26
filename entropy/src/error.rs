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
    #[error("service error")]
    Service(#[from] helium_proto::services::Error),
    #[error("http server error")]
    Server(#[from] hyper::Error),
    #[error("store error")]
    Store(#[from] file_store::Error),
    #[error("env error")]
    Env(#[from] std::env::VarError),
    #[error("mpsc channel error")]
    Channel,
    #[error("not found")]
    NotFound(String),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::DecodeError),
    #[error("bs58 error")]
    Bs58(#[from] bs58::decode::Error),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("socket addr error")]
    SocketAddr(#[from] std::net::AddrParseError),
    #[error("jsonrpc error")]
    JsonRpc(#[from] jsonrpsee::core::Error),
}

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error("prost error")]
    Prost(#[from] helium_proto::EncodeError),
    #[error("json error")]
    Json(#[from] serde_json::Error),
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
from_err!(EncodeError, serde_json::Error);

// Decode Errors
from_err!(DecodeError, http::uri::InvalidUri);
from_err!(DecodeError, prost::DecodeError);
from_err!(DecodeError, jsonrpsee::core::Error);
from_err!(DecodeError, bs58::decode::Error);

impl Error {
    pub fn not_found<E: ToString>(msg: E) -> Self {
        Self::NotFound(msg.to_string())
    }

    pub fn channel() -> Error {
        Error::Channel
    }
}
