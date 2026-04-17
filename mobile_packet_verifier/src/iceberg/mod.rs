use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

pub mod burned_session;
pub mod session;

pub use burned_session::IcebergBurnedDataTransferSession;
pub use session::IcebergDataTransferSession;

pub const NAMESPACE: &str = "data_transfer";

pub type DataTransferWriter = BoxedDataWriter<IcebergDataTransferSession>;
pub type BurnedDataTransferWriter = BoxedDataWriter<IcebergBurnedDataTransferSession>;

pub async fn get_writers(
    settings: &helium_iceberg::Settings,
) -> anyhow::Result<(DataTransferWriter, BurnedDataTransferWriter)> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let session_writer = catalog
        .create_table_if_not_exists(session::table_definition()?)
        .await?;
    let burned_session_writer = catalog
        .create_table_if_not_exists(burned_session::table_definition()?)
        .await?;

    Ok((session_writer.boxed(), burned_session_writer.boxed()))
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
