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
            .with_rows(|_snapshot, _rows| async move { Err::<(), Error>(Error::Writer("boom".into())) })
            .await;

        assert!(result.is_err());
        // The watermark was not advanced, so the snapshot will be re-streamed.
        assert_eq!(state.latest_sequence_number().await.unwrap(), None);
    }
}

#[cfg(all(test, feature = "test-harness"))]
mod tests {
    use super::*;
    use crate::test_harness::IcebergTestHarness;
    use crate::{FieldDefinition, PartitionDefinition, TableDefinition};
    use iceberg::table::Table;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Number {
        name: String,
        value: i64,
    }

    fn numbers_table() -> TableDefinition {
        TableDefinition::builder("default", "numbers")
            .with_fields([
                FieldDefinition::required_string("name"),
                FieldDefinition::required_long("value"),
            ])
            .with_partition(PartitionDefinition::identity("name"))
            .build()
            .expect("table def")
    }

    fn num(name: &str, value: i64) -> Number {
        Number {
            name: name.to_string(),
            value,
        }
    }

    async fn read_snapshot(table: &Table, snapshot_id: i64) -> Vec<Number> {
        let mut rows = added_record_batches(table, snapshot_id)
            .await
            .unwrap()
            .iter()
            .flat_map(|b| batch_to_records::<Number>(b).unwrap())
            .collect::<Vec<_>>();
        rows.sort_by_key(|n| n.value);
        rows
    }

    /// Snapshot ids ordered by sequence number (commit order).
    async fn ordered_snapshot_ids(harness: &IcebergTestHarness) -> Vec<i64> {
        let table = harness
            .iceberg_catalog()
            .load_table("default", "numbers")
            .await
            .expect("load table");
        let mut snaps: Vec<(i64, i64)> = table
            .metadata()
            .snapshots()
            .map(|s| (s.sequence_number(), s.snapshot_id()))
            .collect();
        snaps.sort_by_key(|(seq, _)| *seq);
        snaps.into_iter().map(|(_, id)| id).collect()
    }

    #[tokio::test]
    async fn reader_isolates_rows_added_by_each_snapshot() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([numbers_table()]).await?;
        let writer = harness.get_table_writer::<Number>("numbers").await?;

        writer.write(vec![num("a", 1)]).await?;
        writer.write(vec![num("b", 2), num("c", 3)]).await?;
        writer.write(vec![num("d", 4)]).await?;

        let ids = ordered_snapshot_ids(&harness).await;
        assert_eq!(ids.len(), 3);

        let table = harness
            .iceberg_catalog()
            .load_table("default", "numbers")
            .await?;

        assert_eq!(read_snapshot(&table, ids[0]).await, vec![num("a", 1)]);
        assert_eq!(
            read_snapshot(&table, ids[1]).await,
            vec![num("b", 2), num("c", 3)]
        );
        assert_eq!(read_snapshot(&table, ids[2]).await, vec![num("d", 4)]);

        Ok(())
    }

    #[tokio::test]
    async fn poller_streams_each_snapshot_in_order_and_advances_watermark() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([numbers_table()]).await?;
        let writer = harness.get_table_writer::<Number>("numbers").await?;
        writer.write(vec![num("a", 1)]).await?;
        writer.write(vec![num("b", 2)]).await?;
        writer.write(vec![num("c", 3)]).await?;

        // Let the poller open its own db under this dir.
        let dir = tempfile::tempdir()?;
        let (mut rx, server) = continuous::<Number>()
            .catalog(harness.iceberg_catalog().clone())
            .namespace("default")
            .table("numbers")
            .db_dir(dir.path())
            .poll_duration(Duration::from_millis(200))
            .create()
            .await?;

        let (trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(server.run(listener));

        let mut seen = Vec::new();
        let mut last_seq = i64::MIN;
        for _ in 0..3 {
            let mut event = rx.recv().await.expect("event");
            // Snapshots arrive in strictly increasing sequence-number order.
            assert!(event.snapshot.sequence_number > last_seq);
            last_seq = event.snapshot.sequence_number;

            seen.extend(event.take_rows());
            event.commit().await?;
        }

        seen.sort_by_key(|n| n.value);
        assert_eq!(seen, vec![num("a", 1), num("b", 2), num("c", 3)]);

        trigger.trigger();
        let _ = handle.await?;

        // Watermark advanced to the latest snapshot's sequence number; reopen
        // the poller's db (`default-numbers-default.db`) to read it back.
        let store = StreamState::open(
            dir.path().join("default-numbers-default.db"),
            "default",
            "numbers",
        )
        .await?;
        assert_eq!(store.latest_sequence_number().await?, Some(last_seq));
        Ok(())
    }

    #[tokio::test]
    async fn poller_resumes_after_watermark() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([numbers_table()]).await?;
        let writer = harness.get_table_writer::<Number>("numbers").await?;
        writer.write(vec![num("a", 1)]).await?;
        writer.write(vec![num("b", 2)]).await?;

        // Pre-seed the watermark past the first two snapshots.
        let ids = ordered_snapshot_ids(&harness).await;
        let table = harness
            .iceberg_catalog()
            .load_table("default", "numbers")
            .await?;
        let second = table.metadata().snapshot_by_id(ids[1]).unwrap();
        let second_meta = SnapshotMeta {
            snapshot_id: ids[1],
            sequence_number: second.sequence_number(),
            timestamp: second.timestamp()?,
            operation: second.summary().operation.clone(),
        };

        // Seed the watermark in the db the poller will open, then close it.
        let dir = tempfile::tempdir()?;
        let store = StreamState::open(
            dir.path().join("default-numbers-default.db"),
            "default",
            "numbers",
        )
        .await?;
        store.record(&second_meta).await?;
        drop(store);

        // A third snapshot committed after the seeded watermark.
        writer.write(vec![num("c", 3)]).await?;

        let (mut rx, server) = continuous::<Number>()
            .catalog(harness.iceberg_catalog().clone())
            .namespace("default")
            .table("numbers")
            .db_dir(dir.path())
            .poll_duration(Duration::from_millis(200))
            .create()
            .await?;

        let (trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(server.run(listener));

        let mut event = rx.recv().await.expect("event");
        let seen = event.take_rows();
        event.commit().await?;
        // Only the post-watermark snapshot is replayed.
        assert_eq!(seen, vec![num("c", 3)]);

        trigger.trigger();
        let _ = handle.await?;
        Ok(())
    }
}
