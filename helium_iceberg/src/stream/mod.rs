//! Stream updates to an Iceberg table as events.
//!
//! This is the read-side counterpart to this crate's writers, and the Iceberg
//! analogue of `file_store`'s `file_info_poller`: where that treats each new
//! S3 file as an event, this treats each new Iceberg **snapshot** (commit) as
//! an event — conceptually how Spark Structured Streaming reads an Iceberg
//! table snapshot-by-snapshot.
//!
//! # Usage
//!
//! ```ignore
//! let (mut rx, server) = helium_iceberg::stream::continuous::<MyRow, _>()
//!     .catalog(catalog)
//!     .namespace("poc")
//!     .table("heartbeats")
//!     .state(pool.clone())            // sqlx::PgPool (with the `sqlx-postgres` feature)
//!     .lookback_start_after(start)
//!     .create()
//!     .await?;
//!
//! // Drive `server` with task_manager (ManagedTask), consume `rx` elsewhere:
//! while let Some(event) = rx.recv().await {
//!     let mut txn = pool.begin().await?;
//!     let mut rows = event.into_stream(&mut txn).await?;   // records the watermark in-txn
//!     while let Some(row) = rows.next().await { /* ... */ }
//!     txn.commit().await?;                                 // at-least-once
//! }
//! ```

mod parser;
mod poller;
mod reader;
mod state;

pub use parser::{batch_to_records, IcebergStreamParser, JsonIcebergStreamParser};
pub use poller::{
    IcebergStreamPollerConfig, IcebergStreamPollerConfigBuilder, IcebergStreamPollerServer,
    LookbackBehavior,
};
pub use reader::{added_data_files_to_batches, added_record_batches};
pub use state::{IcebergStreamState, IcebergStreamStateRecorder, SnapshotMeta};

use crate::Result;
use chrono::Utc;
use futures::stream::{BoxStream, StreamExt};

/// One snapshot's worth of newly-appended, decoded rows, plus the snapshot
/// metadata. The Iceberg analogue of `file_store`'s `FileInfoStream`.
#[derive(Debug)]
pub struct IcebergStream<T> {
    pub snapshot: SnapshotMeta,
    process_name: String,
    table_name: String,
    data: Vec<T>,
}

impl<T> IcebergStream<T>
where
    T: Send,
{
    pub(crate) fn new(
        process_name: String,
        table_name: String,
        snapshot: SnapshotMeta,
        data: Vec<T>,
    ) -> Self {
        Self {
            snapshot,
            process_name,
            table_name,
            data,
        }
    }

    /// Number of rows this snapshot added.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn record_metrics(&self) {
        let latency = Utc::now() - self.snapshot.timestamp;
        metrics::gauge!(
            "iceberg-stream-latency",
            "table" => self.table_name.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(latency.num_seconds() as f64);

        metrics::gauge!(
            "iceberg-stream-sequence-number",
            "table" => self.table_name.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(self.snapshot.sequence_number as f64);
    }

    /// Record this snapshot as processed via `recorder` (advancing the
    /// watermark), then yield its rows. Pass a `sqlx::Transaction` so the
    /// watermark advance co-commits with the consumer's own work, giving
    /// at-least-once delivery.
    pub async fn into_stream(
        self,
        recorder: &mut impl IcebergStreamStateRecorder,
    ) -> Result<BoxStream<'static, T>>
    where
        T: 'static,
    {
        self.record_metrics();
        recorder
            .record(&self.process_name, &self.table_name, &self.snapshot)
            .await?;
        Ok(futures::stream::iter(self.data.into_iter()).boxed())
    }
}

/// Start building a continuous snapshot poller that decodes rows with the
/// default serde-based [`JsonIcebergStreamParser`]. `Message` is the row type
/// (`T: serde::de::DeserializeOwned`); `State` is the watermark store
/// (e.g. `sqlx::PgPool`).
pub fn continuous<Message, State>(
) -> IcebergStreamPollerConfigBuilder<Message, State, JsonIcebergStreamParser> {
    IcebergStreamPollerConfigBuilder::default().parser(JsonIcebergStreamParser)
}

#[cfg(all(test, feature = "test-harness"))]
mod tests {
    use super::*;
    use crate::test_harness::IcebergTestHarness;
    use crate::{FieldDefinition, PartitionDefinition, TableDefinition};
    use async_trait::async_trait;
    use iceberg::spec::Operation;
    use iceberg::table::Table;
    use parser::batch_to_records;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Number {
        name: String,
        value: i64,
    }

    /// In-memory watermark store standing in for the Postgres impl.
    #[derive(Clone, Default)]
    struct MemState {
        recorded: Arc<Mutex<Vec<SnapshotMeta>>>,
    }

    #[async_trait]
    impl IcebergStreamState for MemState {
        async fn latest_sequence_number(&self, _p: &str, _t: &str) -> Result<Option<i64>> {
            Ok(self
                .recorded
                .lock()
                .unwrap()
                .iter()
                .map(|s| s.sequence_number)
                .max())
        }
    }

    #[async_trait]
    impl IcebergStreamStateRecorder for MemState {
        async fn record(&mut self, _p: &str, _t: &str, snap: &SnapshotMeta) -> Result<()> {
            self.recorded.lock().unwrap().push(snap.clone());
            Ok(())
        }
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

        let mut state = MemState::default();
        let (mut rx, server) = continuous::<Number, MemState>()
            .catalog(harness.iceberg_catalog().clone())
            .namespace("default")
            .table("numbers")
            .state(state.clone())
            .poll_duration(Duration::from_millis(200))
            .create()
            .await?;

        let (trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(server.run(listener));

        use futures::StreamExt;
        let mut seen = Vec::new();
        let mut last_seq = i64::MIN;
        for _ in 0..3 {
            let event = rx.recv().await.expect("event");
            // Snapshots arrive in strictly increasing sequence-number order.
            assert!(event.snapshot.sequence_number > last_seq);
            last_seq = event.snapshot.sequence_number;

            let mut rows = event.into_stream(&mut state).await?;
            while let Some(row) = rows.next().await {
                seen.push(row);
            }
        }

        seen.sort_by_key(|n| n.value);
        assert_eq!(seen, vec![num("a", 1), num("b", 2), num("c", 3)]);

        // Watermark advanced to the latest snapshot's sequence number.
        let watermark = state.latest_sequence_number("default", "numbers").await?;
        assert_eq!(watermark, Some(last_seq));

        trigger.trigger();
        let _ = handle.await?;
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
        let second_seq = table
            .metadata()
            .snapshot_by_id(ids[1])
            .unwrap()
            .sequence_number();

        let state = MemState {
            recorded: Arc::new(Mutex::new(vec![SnapshotMeta {
                snapshot_id: ids[1],
                sequence_number: second_seq,
                timestamp: chrono::Utc::now(),
                operation: Operation::Append,
            }])),
        };

        // A third snapshot committed after the seeded watermark.
        writer.write(vec![num("c", 3)]).await?;

        let mut state2 = state.clone();
        let (mut rx, server) = continuous::<Number, MemState>()
            .catalog(harness.iceberg_catalog().clone())
            .namespace("default")
            .table("numbers")
            .state(state.clone())
            .poll_duration(Duration::from_millis(200))
            .create()
            .await?;

        let (trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(server.run(listener));

        use futures::StreamExt;
        let event = rx.recv().await.expect("event");
        let mut rows = event.into_stream(&mut state2).await?;
        let mut seen = Vec::new();
        while let Some(row) = rows.next().await {
            seen.push(row);
        }
        // Only the post-watermark snapshot is replayed.
        assert_eq!(seen, vec![num("c", 3)]);

        trigger.trigger();
        let _ = handle.await?;
        Ok(())
    }
}
