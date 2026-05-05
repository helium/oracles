//! Batching layer over [`IcebergTable`] with on-disk spooling.
//!
//! Callers `queue` records into a `BatchedWriter` handle; a background
//! [`BatchedWriterTask`] accumulates them in an Arrow-IPC spool file on
//! disk and commits to Iceberg when either the size or time threshold is
//! reached, on explicit `flush()`, or on shutdown. On startup, any
//! leftover spool files from a prior run are replayed before the writer
//! accepts new traffic.
//!
//! `queue` and `queue_all` await a per-message ack so they don't return
//! until the records have been pushed through `BufWriter` to the kernel
//! page cache. After they return, the records survive a process abort
//! (only a kernel crash or hardware failure loses them). The Iceberg
//! commit itself is still asynchronous — call `flush()` when you need
//! that synchronization point.
//!
//! The spool gives us crash recovery between flushes without a separate
//! WAL: queue → disk → kernel page cache; on restart, replay reads the
//! file back and commits before resuming. See [`spool`] for the file
//! lifecycle and truncation handling.

mod spool;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::Serialize;
use task_manager::{ManagedTask, TaskFuture};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, MissedTickBehavior};

use crate::iceberg_table::{records_to_batch, IcebergTable};
use crate::{Error, Result};
use spool::Spool;

const DEFAULT_MAX_BATCH_SIZE: usize = 10_000;
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CHANNEL_SIZE: usize = 100;

/// Configuration for [`BatchedWriter`].
///
/// `spool_dir` is required; the other knobs default to 10_000 records /
/// 60 s timeout / 100-message channel. There is no `Default` impl
/// because there's no sensible default for `spool_dir`.
#[derive(Debug, Clone)]
pub struct BatchedWriterConfig {
    pub max_batch_size: usize,
    pub batch_timeout: Duration,
    pub channel_size: usize,
    pub spool_dir: PathBuf,
}

impl BatchedWriterConfig {
    pub fn new(spool_dir: impl Into<PathBuf>) -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            channel_size: DEFAULT_CHANNEL_SIZE,
            spool_dir: spool_dir.into(),
        }
    }

    pub fn with_max_batch_size(mut self, n: usize) -> Self {
        self.max_batch_size = n;
        self
    }

    pub fn with_batch_timeout(mut self, d: Duration) -> Self {
        self.batch_timeout = d;
        self
    }

    pub fn with_channel_size(mut self, n: usize) -> Self {
        self.channel_size = n;
        self
    }
}

enum BatchMessage<T> {
    /// `ack` fires once the records have been appended to the spool and
    /// pushed to the kernel page cache (BufWriter::flush). Caller waits
    /// on it before returning from `queue` / `queue_all`.
    Queue(Vec<T>, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<Result<()>>),
}

/// Cloneable handle over a batched, spooled writer to an [`IcebergTable`].
///
/// `queue` and `queue_all` send the records to the background task and
/// wait for an ack — they return only after the records are durable in
/// the kernel page cache. The Iceberg commit itself is still
/// asynchronous; `flush` is the synchronization point for that.
pub struct BatchedWriter<T> {
    sender: mpsc::Sender<BatchMessage<T>>,
}

// Manual `Clone`: deriving would add a spurious `T: Clone` bound even
// though `mpsc::Sender<_>` is `Clone` regardless of `T`.
impl<T> Clone for BatchedWriter<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> BatchedWriter<T>
where
    T: Serialize + Send + Sync + 'static,
{
    /// Build the handle and the background task. Caller registers the
    /// task with `TaskManager` (or spawns `task.run(shutdown)` directly).
    /// Spool replay happens inside `task.run`, not here.
    pub fn new(
        table: IcebergTable<T>,
        config: BatchedWriterConfig,
    ) -> (Self, BatchedWriterTask<T>) {
        let (sender, receiver) = mpsc::channel(config.channel_size);
        let task = BatchedWriterTask {
            table,
            receiver,
            config,
        };
        (Self { sender }, task)
    }

    /// Queue a single record. Returns once the record is appended to
    /// the on-disk spool (kernel page cache) — survives a process abort.
    /// Named `queue` (not `write`) to make it obvious that no Iceberg
    /// commit has happened yet.
    pub async fn queue(&self, record: T) -> Result<()> {
        self.queue_all(vec![record]).await
    }

    /// Queue many records as a single message so they're converted to
    /// one `RecordBatch` together (cheaper than per-record). Returns
    /// once the records are appended to the on-disk spool. Mirrors
    /// `FileSinkClient::write_all`.
    pub async fn queue_all(&self, records: Vec<T>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(BatchMessage::Queue(records, ack_tx))
            .await
            .map_err(|_| Error::Writer("batched writer task closed".into()))?;
        ack_rx
            .await
            .map_err(|_| Error::Writer("batched writer task closed before queue ack".into()))?
    }

    /// Force an immediate flush and wait for the result. Returns the
    /// success/failure of the underlying Iceberg commit.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(BatchMessage::Flush(tx))
            .await
            .map_err(|_| Error::Writer("batched writer task closed".into()))?;
        rx.await
            .map_err(|_| Error::Writer("batched writer task closed before flush ack".into()))?
    }
}

/// Background task that drains the queue, manages the on-disk spool, and
/// commits batches to Iceberg.
pub struct BatchedWriterTask<T> {
    table: IcebergTable<T>,
    receiver: mpsc::Receiver<BatchMessage<T>>,
    config: BatchedWriterConfig,
}

impl<T> BatchedWriterTask<T>
where
    T: Serialize + Send + Sync + 'static,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        let table_label = {
            let guard = self.table.table.read().await;
            let id = guard.identifier();
            format!("{}.{}", id.namespace().to_url_string(), id.name())
        };

        Spool::replay_dir(&self.config.spool_dir, &self.table).await?;
        let mut spool = Spool::create(&self.config.spool_dir, &self.table).await?;

        let mut timer = time::interval(self.config.batch_timeout);
        timer.set_missed_tick_behavior(MissedTickBehavior::Burst);
        timer.tick().await; // discard the immediate first tick

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    flush_to_iceberg(&mut spool, &self.table, &table_label, "shutdown").await?;
                    break;
                }
                _ = timer.tick() => {
                    flush_to_iceberg(&mut spool, &self.table, &table_label, "timeout").await?;
                }
                msg = self.receiver.recv() => match msg {
                    Some(BatchMessage::Queue(records, ack)) => {
                        // Ack as soon as the records are on disk — don't
                        // make the caller wait for any size-triggered
                        // Iceberg commit that follows.
                        let append_res = (async {
                            let batch = {
                                let guard = self.table.table.read().await;
                                records_to_batch(&guard, &records)?
                            };
                            spool.append(batch).await
                        })
                        .await;
                        let append_ok = append_res.is_ok();
                        let _ = ack.send(append_res);
                        if append_ok && spool.record_count() >= self.config.max_batch_size {
                            flush_to_iceberg(&mut spool, &self.table, &table_label, "size").await?;
                            timer.reset();
                        }
                    }
                    Some(BatchMessage::Flush(ack)) => {
                        let res = flush_to_iceberg(&mut spool, &self.table, &table_label, "manual").await;
                        let _ = ack.send(res);
                        timer.reset();
                    }
                    None => {
                        // All handles dropped. Drain and exit.
                        flush_to_iceberg(&mut spool, &self.table, &table_label, "channel_closed")
                            .await?;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl<T> ManagedTask for BatchedWriterTask<T>
where
    T: Serialize + Send + Sync + 'static,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        task_manager::spawn((*self).run(shutdown))
    }
}

/// Flush the spool to Iceberg and emit an info log on success. No-op
/// (and no log) when the spool has no buffered records — empty
/// timer/shutdown ticks shouldn't show up in logs.
async fn flush_to_iceberg<T>(
    spool: &mut Spool,
    table: &IcebergTable<T>,
    table_label: &str,
    reason: &'static str,
) -> Result<()>
where
    T: Serialize + Send + Sync + 'static,
{
    if spool.is_empty() {
        return Ok(());
    }
    let records = spool.record_count();
    let started = Instant::now();
    spool.flush_to_iceberg(table).await?;
    let duration_ms = started.elapsed().as_millis() as u64;
    tracing::info!(
        table = table_label,
        reason,
        records,
        duration_ms,
        "flushed batch to iceberg",
    );
    Ok(())
}
