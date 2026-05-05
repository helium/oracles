use anyhow::Context;

pub mod price_report;

pub use price_report::IcebergPriceReport;

pub const NAMESPACE: &str = "rewards";

/// Type alias for the price-report Iceberg table handle. The server feeds
/// this into a `BatchedWriter`; the backfiller uses it directly via
/// `DataWriter::write_idempotent` (one snapshot per S3 file, keyed by file
/// name for safe replay).
pub type PriceTable = helium_iceberg::IcebergTable<IcebergPriceReport>;

/// Connect to the catalog, ensure the namespace + table exist, and return a
/// table handle.
pub async fn connect_table(settings: &helium_iceberg::Settings) -> anyhow::Result<PriceTable> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let table = catalog
        .create_table_if_not_exists::<IcebergPriceReport>(price_report::table_definition()?)
        .await?;

    Ok(table)
}
