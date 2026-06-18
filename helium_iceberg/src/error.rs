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

    #[error("json (de)serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("snapshot {snapshot_id} not found in table metadata")]
    SnapshotNotFound { snapshot_id: i64 },

    #[error("missing required field building iceberg stream poller: {0}")]
    UninitializedField(#[from] derive_builder::UninitializedFieldError),

    #[cfg(feature = "stream")]
    #[error("watermark store error: {0}")]
    State(#[from] sqlx::Error),

    #[cfg(feature = "stream")]
    #[error("watermark store migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("iceberg stream consumer for {table} ({process_name}) was dropped")]
    ConsumerDropped { table: String, process_name: String },

    #[error(
        "non-append snapshot {snapshot_id} ({operation:?}) encountered while skip_non_append is disabled"
    )]
    NonAppendSnapshot {
        snapshot_id: i64,
        operation: iceberg::spec::Operation,
    },
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
