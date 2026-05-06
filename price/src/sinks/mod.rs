//! Pluggable destinations for `PriceReportV1` records emitted by the
//! daemon. Each tick is fanned out to every configured sink in
//! `PriceGenerator`. Today we have two implementations:
//!
//! - [`S3PriceSink`] writes proto records to S3 via `file_sink`.
//! - [`IcebergPriceSink`] queues records into a `BatchedWriter` for
//!   commit to the `tokens.prices` Iceberg table.
//!
//! Adding another destination (e.g. a webhook, a metric) is a matter of
//! one new impl — `PriceGenerator` doesn't need to learn about it.

use anyhow::Result;
use async_trait::async_trait;
use helium_proto::PriceReportV1;

mod file_store;
mod iceberg;

pub use file_store::S3PriceSink;
pub use iceberg::IcebergPriceSink;

#[async_trait]
pub trait PriceSink: Send + Sync {
    /// Persist a price report. Returning `Err` is fatal — the daemon
    /// crashes so the orchestrator can restart it. Both
    /// `FileSinkClient::write` and `BatchedWriter::queue` only fail
    /// when their backing task is dead, which is unrecoverable in
    /// process. Soft skips (e.g. invalid records) should log and
    /// return `Ok(())`.
    async fn write(&self, report: &PriceReportV1) -> Result<()>;
}
