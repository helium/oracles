//! End-to-end tests for the Iceberg snapshot stream poller.
//!
//! These run against the real Polaris + Trino + S3 harness from
//! `helium_iceberg::test_harness`, and exercise only the public streaming API:
//!
//! 1. **Per-snapshot reads** — `added_record_batches` returns exactly the rows
//!    a given snapshot appended, not the table's full state.
//! 2. **In-order delivery + watermark persistence** — the poller emits new
//!    snapshots in `sequence_number` order, and a second poller on the same
//!    `db_dir` is caught up (emits nothing), proving the watermark persisted.
//! 3. **Resume** — after partial progress, a fresh poller streams only the
//!    snapshots committed after the persisted watermark.
#![cfg(all(feature = "stream", feature = "test-harness"))]

use std::time::Duration;

use helium_iceberg::iceberg::table::Table;
use helium_iceberg::{
    added_record_batches, batch_to_records, continuous, FieldDefinition, IcebergTestHarness,
    PartitionDefinition, TableDefinition,
};
use serde::{Deserialize, Serialize};

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
async fn poller_streams_in_order_and_persists_watermark() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([numbers_table()]).await?;
    let writer = harness.get_table_writer::<Number>("numbers").await?;
    writer.write(vec![num("a", 1)]).await?;
    writer.write(vec![num("b", 2)]).await?;
    writer.write(vec![num("c", 3)]).await?;

    let dir = tempfile::tempdir()?;

    // First poller: stream all three, committing each.
    let (mut rx, server) = continuous::<Number>()
        .catalog(harness.iceberg_catalog().clone())
        .namespace("default")
        .table("numbers")
        .db_dir(dir.path())
        .poll_duration(Duration::from_millis(100))
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
    handle.await??;

    // Second poller on the same db_dir: the watermark persisted, so it's caught
    // up and the stream closes via idle timeout without emitting anything.
    let (mut rx, server) = continuous::<Number>()
        .catalog(harness.iceberg_catalog().clone())
        .namespace("default")
        .table("numbers")
        .db_dir(dir.path())
        .poll_duration(Duration::from_millis(50))
        .idle_timeout(Duration::from_millis(300))
        .create()
        .await?;
    let (_trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(server.run(listener));

    assert!(
        rx.recv().await.is_none(),
        "already-committed snapshots must not be re-emitted"
    );
    handle.await??;

    Ok(())
}

#[tokio::test]
async fn poller_resumes_after_partial_progress() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([numbers_table()]).await?;
    let writer = harness.get_table_writer::<Number>("numbers").await?;
    writer.write(vec![num("a", 1)]).await?;
    writer.write(vec![num("b", 2)]).await?;

    let dir = tempfile::tempdir()?;

    // First poller: drain and commit the two snapshots, then stop on idle.
    let (mut rx, server) = continuous::<Number>()
        .catalog(harness.iceberg_catalog().clone())
        .namespace("default")
        .table("numbers")
        .db_dir(dir.path())
        .poll_duration(Duration::from_millis(50))
        .idle_timeout(Duration::from_millis(300))
        .create()
        .await?;
    let (_trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(server.run(listener));

    let mut committed = 0;
    while let Some(mut event) = rx.recv().await {
        event.take_rows();
        event.commit().await?;
        committed += 1;
    }
    assert_eq!(committed, 2);
    handle.await??;

    // A third snapshot lands after the first poller stopped.
    writer.write(vec![num("c", 3)]).await?;

    // Second poller on the same db_dir resumes past the watermark, emitting
    // only the third snapshot.
    let (mut rx, server) = continuous::<Number>()
        .catalog(harness.iceberg_catalog().clone())
        .namespace("default")
        .table("numbers")
        .db_dir(dir.path())
        .poll_duration(Duration::from_millis(50))
        .idle_timeout(Duration::from_millis(300))
        .create()
        .await?;
    let (_trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(server.run(listener));

    let mut seen = Vec::new();
    while let Some(mut event) = rx.recv().await {
        seen.extend(event.take_rows());
        event.commit().await?;
    }
    assert_eq!(seen, vec![num("c", 3)]);
    handle.await??;

    Ok(())
}
