//! Stream updates to an Iceberg table as events.
//!
//! This is the read-side counterpart to this crate's writers, and the Iceberg
//! analogue of `file_store`'s `file_info_poller`: where that treats each new
//! S3 file as an event, this treats each new Iceberg **snapshot** (commit) as
//! an event — conceptually how Spark Structured Streaming reads an Iceberg
//! table snapshot-by-snapshot.
//!
//! Progress is tracked in a per-poller SQLite db (one file per poller, mount it
//! on a PVC). The poller either opens it for you under a directory you provide
//! (`{namespace}-{table}-{process_name}.db`) or uses a pre-created store.
//!
//! # Usage
//!
//! ```ignore
//! let (mut rx, server) = helium_iceberg::stream::continuous::<MyRow>()
//!     .catalog(catalog)
//!     .namespace("poc")
//!     .table("heartbeats")
//!     .db_dir("/data")                 // opens /data/poc-heartbeats-default.db
//!     .lookback_start_after(start)
//!     .create()
//!     .await?;
//!
//! // Drive `server` with task_manager (ManagedTask), consume `rx` elsewhere.
//! // `with_rows` writes to the sink, then advances the watermark only on success:
//! while let Some(event) = rx.recv().await {
//!     event
//!         .with_rows(|snapshot, rows| async move {
//!             let write_id = format!("heartbeats-{}", snapshot.sequence_number);
//!             sink.write_idempotent(&write_id, transform(rows)).await
//!         })
//!         .await?;
//! }
//! ```

mod parser;
mod poller;
mod reader;
mod state;

pub use parser::batch_to_records;
pub use poller::{
    IcebergStreamPollerConfig, IcebergStreamPollerConfigBuilder, IcebergStreamPollerServer,
    LookbackBehavior,
};
pub use reader::{added_data_files_to_batches, added_record_batches};
pub use state::SnapshotMeta;

use state::StreamState;

use crate::Result;
use chrono::Utc;

/// One snapshot's worth of newly-appended, decoded rows, plus the snapshot
/// metadata and a handle to record progress. The Iceberg analogue of
/// `file_store`'s `FileInfoStream`.
#[derive(Debug)]
pub struct IcebergEvent<T> {
    pub snapshot: SnapshotMeta,
    data: Vec<T>,
    state: StreamState,
}

impl<T> IcebergEvent<T> {
    pub(crate) fn new(state: StreamState, snapshot: SnapshotMeta, data: Vec<T>) -> Self {
        Self {
            snapshot,
            data,
            state,
        }
    }

    /// Number of rows this snapshot added.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Borrow the decoded rows.
    pub fn rows(&self) -> &[T] {
        &self.data
    }

    /// Take ownership of the decoded rows, leaving the event able to `commit`.
    pub fn take_rows(&mut self) -> Vec<T> {
        std::mem::take(&mut self.data)
    }

    fn record_metrics(&self) {
        let latency = Utc::now() - self.snapshot.timestamp;
        metrics::gauge!(
            "iceberg-event-latency",
            "table" => self.state.table_name().to_string(),
            "process-name" => self.state.process_name().to_string(),
        )
        .set(latency.num_seconds() as f64);

        metrics::gauge!(
            "iceberg-event-sequence-number",
            "table" => self.state.table_name().to_string(),
            "process-name" => self.state.process_name().to_string(),
        )
        .set(self.snapshot.sequence_number as f64);
    }

    /// Mark this snapshot processed, advancing the durable watermark.
    ///
    /// Call this **after** your sink write has committed: a crash before
    /// `commit` re-runs the snapshot (at-least-once), which an idempotent sink
    /// (e.g. `write_idempotent` keyed by `snapshot.sequence_number`) absorbs.
    pub async fn commit(self) -> Result<()> {
        self.record_metrics();
        self.state.record(&self.snapshot).await
    }

    /// Hand the snapshot metadata and decoded rows to `f` and, **only if it
    /// succeeds**, commit the watermark — encapsulating the "process, then
    /// advance" ordering.
    ///
    /// If `f` returns an error the watermark is left untouched, so the snapshot
    /// is re-streamed on restart (at-least-once); pair it with an idempotent
    /// sink. A commit failure surfaces through `f`'s error type, which must be
    /// constructible from [`helium_iceberg::Error`](crate::Error).
    ///
    /// ```ignore
    /// event
    ///     .with_rows(|snapshot, rows| async move {
    ///         let write_id = format!("heartbeats-{}", snapshot.sequence_number);
    ///         sink.write_idempotent(&write_id, transform(rows)).await
    ///     })
    ///     .await?;
    /// ```
    pub async fn with_rows<F, Fut, R, E>(mut self, f: F) -> std::result::Result<R, E>
    where
        F: FnOnce(SnapshotMeta, Vec<T>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<R, E>>,
        E: From<crate::Error>,
    {
        let snapshot = self.snapshot.clone();
        let output = f(snapshot, self.take_rows()).await?;
        self.commit().await?;
        Ok(output)
    }
}

/// Start building a continuous snapshot poller. `Message` is the row type
/// (`T: serde::de::DeserializeOwned`, decoded from each Arrow batch via
/// [`batch_to_records`]). Progress is persisted to a per-poller SQLite db
/// (see [`IcebergStreamPollerConfigBuilder::db_dir`] /
/// [`IcebergStreamPollerConfigBuilder::pool`]).
pub fn continuous<Message>() -> IcebergStreamPollerConfigBuilder<Message> {
    IcebergStreamPollerConfigBuilder::default()
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use crate::Error;
    use chrono::Utc;
    use iceberg::spec::Operation;

    fn event(state: StreamState, sequence_number: i64, rows: Vec<i64>) -> IcebergEvent<i64> {
        IcebergEvent::new(
            state,
            SnapshotMeta {
                snapshot_id: sequence_number * 10,
                sequence_number,
                timestamp: Utc::now(),
                operation: Operation::Append,
            },
            rows,
        )
    }

    #[tokio::test]
    async fn with_rows_commits_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let state = StreamState::open(dir.path().join("wm.db"), "p", "t")
            .await
            .unwrap();

        let seen: Vec<i64> = event(state.clone(), 1, vec![10, 20])
            .with_rows(|_snapshot, rows| async move { Ok::<_, Error>(rows) })
            .await
            .unwrap();

        assert_eq!(seen, vec![10, 20]);
        assert_eq!(state.latest_sequence_number().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn with_rows_skips_commit_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let state = StreamState::open(dir.path().join("wm.db"), "p", "t")
            .await
            .unwrap();

        let result = event(state.clone(), 1, vec![10])
            .with_rows(
                |_snapshot, _rows| async move { Err::<(), Error>(Error::Writer("boom".into())) },
            )
            .await;

        assert!(result.is_err());
        // The watermark was not advanced, so the snapshot will be re-streamed.
        assert_eq!(state.latest_sequence_number().await.unwrap(), None);
    }
}
