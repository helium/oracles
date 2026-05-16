use crate::{
    backfill::BatchedWriterExt,
    iceberg::{self, IcebergSpeedtestAvg},
    speedtests_average::SpeedtestAvgBackfiller,
    Settings,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use helium_iceberg::BatchedWriterConfig;
use std::time::Duration;
use task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Process name for tracking speedtest_avg backfill (avoids conflict with daemon)
    #[clap(long, default_value = "speedtest-avg-backfill")]
    process_name: String,

    /// Start processing files after this timestamp.
    /// Format: RFC 3339 (e.g., 2024-01-01T00:00:00Z)
    #[clap(long)]
    start_after: DateTime<Utc>,

    /// Stop processing files when their timestamp is > this value.
    /// Use this to avoid reprocessing files that the daemon has already handled.
    /// Format: RFC 3339 (e.g., 2025-02-25T00:00:00Z)
    #[clap(long)]
    stop_after: DateTime<Utc>,

    /// Batch size for processing files.
    #[clap(long)]
    batch_size: Option<usize>,

    /// Max time the batched writer waits before flushing a partial batch.
    /// Accepts human-readable durations like "5m", "30s", "1h".
    #[clap(long, value_parser = humantime::parse_duration)]
    batch_timeout: Option<Duration>,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let pool = settings
            .database
            .connect("mobile-verifier-speedtest-avg-backfill")
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let iceberg_settings = settings.iceberg_settings.as_ref().ok_or_else(|| {
            anyhow::anyhow!("iceberg_settings required for speedtest_avg backfill")
        })?;

        let (writer, writer_task) = IcebergSpeedtestAvg::batched_writer(
            iceberg_settings.connect().await?,
            iceberg::speedtest_avg::table_definition()?,
            BatchedWriterConfig::new(settings.cache.join("iceberg-spool/speedtest-avg-backfill"))
                .with_max_batch_size_opt(self.batch_size)
                .with_batch_timeout_opt(self.batch_timeout),
        )
        .await?;

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            "starting speedtest_avg backfill"
        );

        let opts = crate::backfill::BackfillOptions {
            process_name: self.process_name,
            start_after: self.start_after,
            stop_after: self.stop_after,
            poll_duration: None,
            idle_timeout: None,
        };

        let (backfiller, server) = SpeedtestAvgBackfiller::create_batched(
            pool,
            settings.buckets.output.connect().await,
            writer,
            opts,
        )
        .await?;

        TaskManager::builder()
            .add_task(writer_task)
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start()
            .await?;
        Ok(())
    }
}
