use crate::sinks::PriceSink;
use anyhow::Result;
use async_trait::async_trait;
use file_store::file_sink::FileSinkClient;
use helium_proto::PriceReportV1;

/// Writes each tick to S3 via the existing `file_sink` (rolled into
/// `PriceReportV1.<n>.gz` files on the schedule configured in
/// `Server::run`).
pub struct S3PriceSink {
    sink: FileSinkClient<PriceReportV1>,
}

impl S3PriceSink {
    pub fn new(sink: FileSinkClient<PriceReportV1>) -> Self {
        Self { sink }
    }
}

#[async_trait]
impl PriceSink for S3PriceSink {
    async fn write(&self, report: &PriceReportV1) -> Result<()> {
        // `PriceReportV1` is a small `Copy` struct — deref to hand it
        // by value into the file_sink without taking ownership at the
        // trait surface.
        self.sink.write(*report, []).await?;
        Ok(())
    }
}
