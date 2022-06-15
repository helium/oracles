use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
    #[error("sql error")]
    Sql(#[from] sqlx::Error),
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
}
