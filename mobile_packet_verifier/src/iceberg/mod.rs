use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

pub mod burned_session;
pub mod invalid_session;
pub mod session;

pub use burned_session::IcebergBurnedDataTransferSession;
pub use invalid_session::IcebergInvalidDataTransferSession;
pub use session::IcebergDataTransferSession;

pub const NAMESPACE: &str = "data_transfer";

/// Column appended to the `invalid_sessions` table, recording why a session was
/// rejected (a `ReportStatus` string name).
pub const REASON_COLUMN: &str = "reason";

// Valid sessions go to `data_transfer.sessions`; rejected sessions go to the
// sibling `data_transfer.invalid_sessions` (same schema plus a `reason` column).
// Burned sessions have no invalid counterpart.
pub type DataTransferWriter = BoxedDataWriter<IcebergDataTransferSession>;
pub type InvalidDataTransferWriter = BoxedDataWriter<IcebergInvalidDataTransferSession>;
pub type BurnedDataTransferWriter = BoxedDataWriter<IcebergBurnedDataTransferSession>;

pub async fn get_writers(
    settings: &helium_iceberg::Settings,
) -> anyhow::Result<(
    DataTransferWriter,
    InvalidDataTransferWriter,
    BurnedDataTransferWriter,
)> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let session_writer = catalog
        .create_table_if_not_exists(session::table_definition()?)
        .await?;
    let invalid_session_writer = catalog
        .create_table_if_not_exists(invalid_session::table_definition()?)
        .await?;
    let burned_session_writer = catalog
        .create_table_if_not_exists(burned_session::table_definition()?)
        .await?;

    Ok((
        session_writer.boxed(),
        invalid_session_writer.boxed(),
        burned_session_writer.boxed(),
    ))
}

/// Optional idempotent append — no-op when `writer` is `None` (iceberg
/// writes are optional in some deployments).
pub async fn maybe_write_idempotent<T: Serialize + Send + 'static>(
    writer: Option<&BoxedDataWriter<T>>,
    id: &str,
    records: Vec<T>,
) -> anyhow::Result<()> {
    if let Some(data_writer) = writer {
        data_writer
            .write_idempotent(id, records)
            .await
            .context("writing idempotent")?;
    }
    Ok(())
}
