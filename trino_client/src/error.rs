#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to build trino client: {0}")]
    Build(String),

    #[error("missing bind parameter: {0}")]
    MissingParameter(String),

    #[error("invalid parameter value: {0}")]
    InvalidParam(String),

    #[error(transparent)]
    Client(#[from] trino_rust_client::error::Error),
}

pub type Result<T = ()> = std::result::Result<T, Error>;
