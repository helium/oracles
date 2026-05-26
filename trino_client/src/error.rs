#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing bind parameter: {0}")]
    MissingParameter(String),

    #[error("invalid parameter value: {0}")]
    InvalidParam(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    JwtWatcher(#[from] crate::jwt_watcher::JwtWatcherError),

    #[error(transparent)]
    Client(#[from] trino_rust_client::error::Error),
}

pub type Result<T = ()> = std::result::Result<T, Error>;
