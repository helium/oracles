use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Sql error")]
    SqlError(#[from] sqlx::Error),
    #[error("Failed to decode value")]
    DecodeError,
    #[error("meta key not found {0}")]
    NotFound(String),
}
