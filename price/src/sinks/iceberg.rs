use crate::iceberg::IcebergPriceReport;
use crate::sinks::PriceSink;
use anyhow::Result;
use async_trait::async_trait;
use helium_iceberg::BatchedWriter;
use helium_proto::PriceReportV1;

/// Queues each tick into a `BatchedWriter` keyed on the
/// `tokens.prices` Iceberg table. The actual snapshot commit is async
/// and governed by the writer's batch-size / batch-timeout knobs.
pub struct IcebergPriceSink {
    writer: BatchedWriter<IcebergPriceReport>,
}

impl IcebergPriceSink {
    pub fn new(writer: BatchedWriter<IcebergPriceReport>) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl PriceSink for IcebergPriceSink {
    async fn write(&self, report: &PriceReportV1) -> Result<()> {
        // `try_from` is the proto → iceberg conversion (sets the
        // `price_usd` Decimal column from the token's decimals). A
        // conversion failure is data-shape, not sink-shape; log and
        // skip rather than crashing the daemon.
        let record = match IcebergPriceReport::try_from(report) {
            Ok(record) => record,
            Err(err) => {
                tracing::error!(?err, "invalid iceberg record; skipping");
                return Ok(());
            }
        };
        self.writer.queue(record).await?;
        Ok(())
    }
}
