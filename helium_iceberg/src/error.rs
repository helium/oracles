use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("table not found: {namespace}.{table}")]
    TableNotFound { namespace: String, table: String },

    #[error("writer error: {0}")]
    Writer(String),

    #[error("branch error: {0}")]
    Branch(String),
}
