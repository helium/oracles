//! Backfill the local Trino/iceberg `data_transfer.burned_sessions` table from
//! the `valid_data_transfer_session` files in an S3 bucket, so a local Trino can
//! be populated with a past window of burned data-transfer sessions (e.g. to run
//! the `compare_*` examples).
//!
//! Uses a FileInfoPoller (`file_source::continuous_source`) over the window, the
//! same shape as the production backfill jobs and `mobile_verifier/src/data_session.rs`
//! — but with a no-op state store (`DropState`) in place of Postgres, so no
//! `files_processed` DB is needed. The trade-off is no resume / no idempotency: a
//! re-run reprocesses the whole window and duplicates rows, so run it once against
//! an empty table. Decoded sessions are queued into a `BatchedWriter` (spool ->
//! batched Iceberg commits). `--before` maps to the poller's `stop_after`, which
//! bounds the run so the poller exits instead of polling forever.
//!
//! Files are processed one at a time (the standard `ChannelConsumer`); the poller
//! prefetches a few ahead, but this is not a concurrent-download path.
//!
//! Source: `settings.buckets.data_transfer` — point this at the bucket holding the
//! burned-session files. Target: `settings.iceberg_settings` (local catalog). A
//! deliberately tiny stand-in for the removed backfill jobs — delete it once the
//! table is populated.
//!
//! Usage:
//!   cargo run -p mobile-verifier --example backfill_burned_sessions -- \
//!     --config <settings.toml> \
//!     --after 2026-06-01T00:00:00Z --before 2026-06-15T00:00:00Z

use chrono::{DateTime, Utc};
use clap::Parser;
use file_store::{
    file_info_poller::{FileInfoPollerState, FileInfoPollerStateRecorder, FileInfoStream},
    file_source, FileInfo,
};
use file_store_oracles::{mobile_transfer::ValidDataTransferSession, FileType};
use futures::StreamExt;
use helium_iceberg::{BatchedWriter, BatchedWriterConfig};
use helium_iceberg_oracles::data_transfer::burned_session::{
    self, IcebergBurnedDataTransferSession,
};
use mobile_verifier::Settings;
use task_manager::{ChannelConsumer, TaskManager};
use tokio::sync::mpsc::Receiver;

#[derive(Parser)]
struct Args {
    #[clap(short = 'c', long)]
    config: std::path::PathBuf,
    /// Backfill files written after this time (RFC3339).
    #[clap(long)]
    after: String,
    /// Stop once a file's timestamp passes this time (RFC3339) — the poller's
    /// `stop_after`, which bounds the run.
    #[clap(long)]
    before: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let after = DateTime::parse_from_rfc3339(&args.after)?.with_timezone(&Utc);
    let before = DateTime::parse_from_rfc3339(&args.before)?.with_timezone(&Utc);
    let settings = Settings::new(Some(&args.config))?;

    // Target: local iceberg catalog -> data_transfer.burned_sessions.
    let iceberg = settings
        .iceberg_settings
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("settings.iceberg_settings must be configured"))?;
    let catalog = iceberg.connect().await?;
    catalog
        .create_namespace_if_not_exists(burned_session::NAMESPACE)
        .await?;
    let table = catalog
        .create_table_if_not_exists::<IcebergBurnedDataTransferSession>(
            burned_session::table_definition()?,
        )
        .await?;
    let config = BatchedWriterConfig::new(
        settings
            .cache
            .join("iceberg-spool/burned-sessions-backfill"),
    );
    let (writer, batched_task) = BatchedWriter::new(table, config);

    // Source: a FileInfoPoller over the window with a no-op state store, so no
    // Postgres is needed. `stop_after` makes it exit once it passes `--before`
    // instead of polling forever. Same builder as `mobile_verifier/src/data_session.rs`.
    let bucket = settings.buckets.data_transfer.connect().await;
    let (receiver, server) = file_source::continuous_source::<ValidDataTransferSession, _, _>()
        .state(DropState)
        .bucket_client(bucket)
        .prefix(FileType::ValidDataTransferSession.to_string())
        .lookback_start_after(after)
        .stop_after(before)
        .create()
        .await?;

    let backfiller = BurnedSessionBackfiller { receiver, writer };

    println!(
        "backfilling {after} .. {before} -> {}.{}",
        burned_session::NAMESPACE,
        burned_session::TABLE_NAME,
    );

    // FIFO start / LIFO shutdown. On natural completion (`--before` reached) the
    // poller drops its sender → the backfiller's channel closes and it exits → its
    // `BatchedWriter` handle drops → the `BatchedWriterTask` drains its spool,
    // commits the final batch, and exits cleanly.
    TaskManager::builder()
        .add_task(batched_task)
        .add_task(server)
        .add_task(task_manager::channel_consumer(backfiller))
        .build()
        .start()
        .await?;

    println!("done");
    Ok(())
}

/// Streams each `valid_data_transfer_session` file's sessions into the iceberg
/// `BatchedWriter`.
struct BurnedSessionBackfiller {
    receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
    writer: BatchedWriter<IcebergBurnedDataTransferSession>,
}

impl BurnedSessionBackfiller {
    async fn process(&self, file: FileInfoStream<ValidDataTransferSession>) -> anyhow::Result<()> {
        let key = file.file_info.key.clone();
        let timestamp = file.file_info.timestamp;

        // `into_stream` wants a recorder to mark the file processed; hand it a
        // throwaway `DropState` that swallows it (no DB), then yield the
        // already-decoded sessions, convert, and queue them.
        let mut recorder = DropState;
        let rows: Vec<_> = file
            .into_stream(&mut recorder)
            .await?
            .map(IcebergBurnedDataTransferSession::from)
            .collect()
            .await;
        let n = rows.len();
        self.writer.queue_all(rows).await?;

        println!("  {key} -> {n} sessions {:?}", timestamp);
        Ok(())
    }
}

impl ChannelConsumer for BurnedSessionBackfiller {
    type Item = FileInfoStream<ValidDataTransferSession>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.receiver.recv().await
    }

    async fn handle(&mut self, file: Self::Item) -> anyhow::Result<()> {
        self.process(file).await
    }
}

/// No-op poller state: claims nothing has been processed and discards every
/// "processed" record, so the backfill runs without a Postgres `files_processed`
/// store. Trade-off vs a real DB: no resume / no idempotency — a re-run
/// reprocesses the whole window (and duplicates rows). Fine for a one-shot.
#[derive(Clone, Copy)]
struct DropState;

#[async_trait::async_trait]
impl FileInfoPollerState for DropState {
    async fn latest_timestamp(
        &self,
        _process_name: &str,
        _file_type: &str,
    ) -> file_store::Result<Option<DateTime<Utc>>> {
        Ok(None) // no persisted progress -> start from lookback_start_after
    }

    async fn exists(&self, _process_name: &str, _file_info: &FileInfo) -> file_store::Result<bool> {
        Ok(false) // nothing is ever "already processed"
    }

    async fn clean(
        &self,
        _process_name: &str,
        _file_type: &str,
        _offset: DateTime<Utc>,
    ) -> file_store::Result<u64> {
        Ok(0)
    }
}

#[async_trait::async_trait]
impl FileInfoPollerStateRecorder for DropState {
    async fn record(
        &mut self,
        _process_name: &str,
        _file_info: &FileInfo,
    ) -> file_store::Result<()> {
        Ok(()) // drop it on the floor
    }
}
