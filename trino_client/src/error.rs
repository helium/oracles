#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to build trino client: {0}")]
    Build(String),

    #[error("missing bind parameter: {0}")]
    MissingParameter(String),

    #[error("invalid parameter value: {0}")]
    InvalidParam(String),

    #[error(transparent)]
    Client(trino_rust_client::error::Error),

    #[error("empty data")]
    EmptyData,
}

impl From<trino_rust_client::error::Error> for Error {
    fn from(err: trino_rust_client::error::Error) -> Self {
        match err {
            trino_rust_client::error::Error::EmptyData => Error::EmptyData,
            other => Error::Client(other),
        }
    }
}

pub type Result<T = ()> = std::result::Result<T, Error>;
