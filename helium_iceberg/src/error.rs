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

pub trait IntoHeliumIcebergError<T> {
    fn err_into(self) -> Result<T>;
}

impl<T, E> IntoHeliumIcebergError<T> for std::result::Result<T, E>
where
    Error: From<E>,
{
    fn err_into(self) -> Result<T> {
        self.map_err(|e| e.into())
    }
}
