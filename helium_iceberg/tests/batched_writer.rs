//! End-to-end tests for `BatchedWriter`.
//!
//! These run against the real Polaris + Trino + S3 harness provided by
//! `helium_iceberg::test_harness`. They cover:
//!
//! - Size-triggered flushes
//! - Time-triggered flushes
//! - Manual `flush()`
//! - Graceful shutdown drain
//! - Crash-recovery replay (records spooled to disk before a hard abort
//!   are committed when a fresh task starts up against the same spool dir)

use std::time::Duration;

use chrono::{DateTime, DurationRound, FixedOffset, Utc};
use helium_iceberg::{
    BatchedWriter, BatchedWriterConfig, FieldDefinition, IcebergTable, IcebergTestHarness,
    PartitionDefinition, TableDefinition,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use trino_rust_client::Trino;

const NAMESPACE: &str = "default";
const TABLE: &str = "people";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
struct Person {
    name: String,
    age: u32,
    inserted: DateTime<FixedOffset>,
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
struct Count {
    c: i64,
}

fn utc_now() -> DateTime<FixedOffset> {
    // Iceberg truncates to microseconds; round so equality holds against
    // what Trino returns.
    Utc::now()
        .duration_trunc(chrono::Duration::microseconds(1))
        .expect("round timestamp")
        .into()
}

fn person_table_def() -> anyhow::Result<TableDefinition> {
    Ok(TableDefinition::builder(NAMESPACE, TABLE)
        .with_fields([
            FieldDefinition::required_string("name"),
            FieldDefinition::required_int("age"),
            FieldDefinition::required_timestamptz("inserted"),
        ])
        .with_partition(PartitionDefinition::day("inserted", "inserted_day"))
        .build()?)
}

async fn fresh_table(harness: &IcebergTestHarness) -> anyhow::Result<IcebergTable<Person>> {
    Ok(
        IcebergTable::<Person>::from_catalog(harness.iceberg_catalog().clone(), NAMESPACE, TABLE)
            .await?,
    )
}

async fn count_in_iceberg(harness: &IcebergTestHarness) -> anyhow::Result<i64> {
    let counts: Vec<Count> = harness
        .trino()
        .get_all::<Count>(format!("SELECT count(*) AS c FROM {NAMESPACE}.{TABLE}"))
        .await?
        .into_vec();
    Ok(counts.first().map(|c| c.c).unwrap_or(0))
}

async fn count_spool_files(spool_dir: &std::path::Path) -> anyhow::Result<usize> {
    if !spool_dir.exists() {
        return Ok(0);
    }
    let mut entries = tokio::fs::read_dir(spool_dir).await?;
    let mut count = 0;
    while entries.next_entry().await?.is_some() {
        count += 1;
    }
    Ok(count)
}

fn person(n: usize) -> Person {
    Person {
        name: format!("p{n:03}"),
        age: n as u32,
        inserted: utc_now(),
    }
}

#[tokio::test]
async fn flushes_when_size_threshold_reached() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let spool_dir = TempDir::new()?;

    let table = fresh_table(&harness).await?;
    let config = BatchedWriterConfig::new(spool_dir.path())
        .with_max_batch_size(5)
        .with_batch_timeout(Duration::from_secs(3600));

    let (writer, task) = BatchedWriter::new(table, config);
    let (trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(task.run(listener));

    for i in 0..5 {
        writer.queue(person(i)).await?;
    }

    // Size-triggered flush is async; poll briefly until visible.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if count_in_iceberg(&harness).await? == 5 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("size-triggered flush did not commit 5 rows in time");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    trigger.trigger();
    drop(writer);
    handle.await??;

    assert_eq!(count_spool_files(spool_dir.path()).await?, 0);
    Ok(())
}

#[tokio::test]
async fn flushes_on_batch_timeout() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let spool_dir = TempDir::new()?;

    let table = fresh_table(&harness).await?;
    let config = BatchedWriterConfig::new(spool_dir.path())
        .with_max_batch_size(10_000)
        .with_batch_timeout(Duration::from_millis(150));

    let (writer, task) = BatchedWriter::new(table, config);
    let (trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(task.run(listener));

    writer
        .queue_all(vec![person(0), person(1), person(2)])
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if count_in_iceberg(&harness).await? == 3 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timeout-triggered flush did not commit 3 rows in time");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    trigger.trigger();
    drop(writer);
    handle.await??;
    Ok(())
}

#[tokio::test]
async fn flush_method_commits_immediately() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let spool_dir = TempDir::new()?;

    let table = fresh_table(&harness).await?;
    let config = BatchedWriterConfig::new(spool_dir.path())
        .with_max_batch_size(10_000)
        .with_batch_timeout(Duration::from_secs(3600));

    let (writer, task) = BatchedWriter::new(table, config);
    let (trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(task.run(listener));

    writer
        .queue_all(vec![person(0), person(1), person(2)])
        .await?;
    writer.flush().await?;

    // After `flush().await?` returns, the rows must already be visible —
    // no polling required.
    assert_eq!(count_in_iceberg(&harness).await?, 3);

    trigger.trigger();
    drop(writer);
    handle.await??;

    assert_eq!(count_spool_files(spool_dir.path()).await?, 0);
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_partial_buffer() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let spool_dir = TempDir::new()?;

    let table = fresh_table(&harness).await?;
    let config = BatchedWriterConfig::new(spool_dir.path())
        .with_max_batch_size(10_000)
        .with_batch_timeout(Duration::from_secs(3600));

    let (writer, task) = BatchedWriter::new(table, config);
    let (trigger, listener) = triggered::trigger();
    let handle = tokio::spawn(task.run(listener));

    writer.queue_all(vec![person(0), person(1)]).await?;
    // `queue_all` returning means the records are already in the spool —
    // no extra wait needed before signalling shutdown.

    trigger.trigger();
    drop(writer);
    handle.await??;

    assert_eq!(count_in_iceberg(&harness).await?, 2);
    assert_eq!(count_spool_files(spool_dir.path()).await?, 0);
    Ok(())
}

/// Records that landed in the spool but were never flushed to Iceberg
/// (process aborted, no graceful shutdown) must be replayed by the next
/// task that starts up against the same `spool_dir`.
#[tokio::test]
async fn replays_leftover_spool_on_startup() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let spool_dir = TempDir::new()?;

    let people = vec![person(0), person(1), person(2)];

    // Phase 1: spool records, then abort the task without flushing.
    {
        let table = fresh_table(&harness).await?;
        let config = BatchedWriterConfig::new(spool_dir.path())
            .with_max_batch_size(10_000)
            .with_batch_timeout(Duration::from_secs(3600));
        let (writer, task) = BatchedWriter::new(table, config);
        let (_trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(task.run(listener));

        // `queue_all` returns only after the records are appended to
        // the spool (kernel page cache via per-append BufWriter::flush),
        // so by the time it resolves they survive an abort.
        writer.queue_all(people.clone()).await?;

        // Abort first, *then* drop the handle. Dropping the writer first
        // would close the channel and trigger the graceful drain branch.
        handle.abort();
        let _ = handle.await;
        drop(writer);
    }

    // Iceberg should still be empty: no flush happened in phase 1.
    assert_eq!(count_in_iceberg(&harness).await?, 0);
    // The spool file is still there waiting for replay.
    assert!(count_spool_files(spool_dir.path()).await? >= 1);

    // Phase 2: fresh task replays before accepting new work.
    {
        let table = fresh_table(&harness).await?;
        let config = BatchedWriterConfig::new(spool_dir.path())
            .with_max_batch_size(10_000)
            .with_batch_timeout(Duration::from_secs(3600));
        let (writer, task) = BatchedWriter::new(table, config);
        let (trigger, listener) = triggered::trigger();
        let handle = tokio::spawn(task.run(listener));

        // `flush()` is processed by the run-loop *after* `replay_dir`,
        // so when this returns the replayed records are committed.
        writer.flush().await?;

        trigger.trigger();
        drop(writer);
        handle.await??;
    }

    let mut expected = people;
    expected.sort_by(|a, b| a.name.cmp(&b.name));
    let queried: Vec<Person> = harness
        .trino()
        .get_all::<Person>(format!("SELECT * FROM {NAMESPACE}.{TABLE} ORDER BY name"))
        .await?
        .into_vec();
    assert_eq!(queried, expected);

    assert_eq!(count_spool_files(spool_dir.path()).await?, 0);
    Ok(())
}
