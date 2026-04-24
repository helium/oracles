use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

pub mod price_report;

pub use price_report::IcebergPriceReport;

pub const NAMESPACE: &str = "rewards";

pub type PriceWriter = BoxedDataWriter<IcebergPriceReport>;

pub async fn get_writer(settings: &helium_iceberg::Settings) -> anyhow::Result<PriceWriter> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let writer = catalog
        .create_table_if_not_exists(price_report::table_definition()?)
        .await?;

    Ok(writer.boxed())
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
