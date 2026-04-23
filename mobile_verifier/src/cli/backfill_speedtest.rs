use crate::{iceberg, speedtests::SpeedtestBackfiller, Settings};
use anyhow::Result;
use chrono::{DateTime, Utc};
use task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Process name for tracking speedtest backfill (avoids conflict with daemon)
    #[clap(long, default_value = "speedtest-backfill")]
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
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let pool = settings
            .database
            .connect("mobile-verifier-speedtest-backfill")
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let iceberg_settings = settings
            .iceberg_settings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("iceberg_settings required for speedtest backfill"))?;

        let speedtest_writer = iceberg::PocWriters::from_settings(iceberg_settings)
            .await?
            .speedtest
            .expect("iceberg writer");

        tracing::info!(
            process_name = %self.process_name,
            start_after = %self.start_after,
            stop_after = %self.stop_after,
            "starting speedtest backfill"
        );

        let opts = crate::backfill::BackfillOptions {
            process_name: self.process_name,
            start_after: self.start_after,
            stop_after: self.stop_after,
            poll_duration: None,
            idle_timeout: None,
        };

        let (backfiller, server) = SpeedtestBackfiller::create(
            pool,
            settings.buckets.output.connect().await,
            Some(speedtest_writer),
            Some(opts),
        )
        .await?;

        TaskManager::builder()
            .add_task(server)
            .add_task(backfiller)
            .build()
            .start()
            .await?;
        Ok(())
    }
}
