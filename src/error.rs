use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
    #[error("sql error")]
    Sql(#[from] sqlx::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("migration error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("http server error")]
    Server(#[from] hyper::Error),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("service error")]
    Service(#[from] helium_proto::services::Error),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("json error")]
    Json(#[from] serde_json::Error),

    #[error("not found")]
    NotFound(String),
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
        match v {
            Error::NotFound(msg) => (http::StatusCode::NOT_FOUND, msg),
            err => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        }
    }
}
