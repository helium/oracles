use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, BranchTransaction, IntoBoxedDataWriter};
use serde::Serialize;

pub mod burned_session;
pub mod session;

pub use burned_session::IcebergBurnedDataTransferSession;
pub use session::IcebergDataTransferSession;

pub const NAMESPACE: &str = "data_transfer";

pub type DataTransferWriter = BoxedDataWriter<IcebergDataTransferSession>;
pub type DataTransferTransaction = BranchTransaction<IcebergDataTransferSession>;

// BurnedDataSessions are not written in transactions (yet (maybe)), so they
// don't have the associated BurnedDataTransferTransaction type alias like
// above.
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

// NOTE(mj): Helpers for dealing with Optional Iceberg writes. These should go
// away when writing to iceberg becomes standard.
pub async fn maybe_begin<T: Serialize + Send + 'static>(
    writer: Option<&BoxedDataWriter<T>>,
    wap_id: &str,
) -> anyhow::Result<Option<BranchTransaction<T>>> {
    let Some(data_writer) = writer else {
        return Ok(None);
    };

    let txn = data_writer.begin(wap_id).await?;
    Ok(Some(txn))
}

pub async fn maybe_publish<T: Serialize + Send + 'static>(
    txn: Option<BranchTransaction<T>>,
) -> anyhow::Result<()> {
    if let Some(txn) = txn {
        txn.publish().await.context("publishing")?;
    }
    Ok(())
}
