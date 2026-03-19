use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, BranchTransaction, IntoBoxedDataWriter};
use serde::Serialize;

pub mod heartbeat;

pub use heartbeat::IcebergHeartbeat;

pub const NAMESPACE: &str = "poc";

pub type HeartbeatWriter = BoxedDataWriter<IcebergHeartbeat>;
pub type HeartbeatTransaction = BranchTransaction<IcebergHeartbeat>;

pub async fn get_writer(settings: &helium_iceberg::Settings) -> anyhow::Result<HeartbeatWriter> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let writer = catalog
        .create_table_if_not_exists(heartbeat::table_definition()?)
        .await?;

    Ok(writer.boxed())
}

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
