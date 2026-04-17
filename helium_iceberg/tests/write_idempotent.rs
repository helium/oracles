//! End-to-end tests for `DataWriter::write_idempotent`.
//!
//! These run against the real Polaris + Trino + S3 harness provided by
//! `helium_iceberg::test_harness`. They validate three properties that the
//! `write_idempotent` contract is meant to guarantee:
//!
//! 1. **Cross-instance idempotency** — a fresh `IcebergTable` must still
//!    skip a duplicate id by consulting the catalog, not in-memory state.
//! 2. **Concurrent-writer safety** — two parallel writers must compose via
//!    `fast_append`'s retry-on-conflict path without clobbering each other.
//! 3. **External-commit preservation** — a `DELETE` that lands on main
//!    between two idempotent writes must not be reverted.

use std::sync::Arc;

use chrono::{DateTime, Duration, DurationRound, FixedOffset, Utc};
use helium_iceberg::{
    DataWriter, FieldDefinition, IcebergTestHarness, PartitionDefinition, TableDefinition,
};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
struct Person {
    name: String,
    age: u32,
    inserted: DateTime<FixedOffset>,
}

/// Linux provides nanosecond precision, Darwin microsecond. Iceberg truncates
/// to microsecond, so round before comparing to what Trino returns.
fn utc_now() -> DateTime<FixedOffset> {
    Utc::now()
        .duration_trunc(Duration::microseconds(1))
        .expect("round timestamp")
        .into()
}

fn person_table_def() -> anyhow::Result<TableDefinition> {
    Ok(TableDefinition::builder("default", "people")
        .with_fields([
            FieldDefinition::required_string("name"),
            FieldDefinition::required_int("age"),
            FieldDefinition::required_timestamptz("inserted"),
        ])
        .with_partition(PartitionDefinition::day("inserted", "inserted_day"))
        .build()?)
}

/// Idempotency must survive a process restart: the second `write_idempotent`
/// call uses a **fresh** `IcebergTable` instance (separate `get_table_writer`
/// call) so its metadata cache is empty and `has_write_id` must hit the
/// catalog to find the existing snapshot. This is the production scenario
/// the idempotency guarantee is actually protecting against — a crash
/// between submit and commit-recording, followed by restart and retry.
///
/// Also verifies an interleaved plain `write` between the two idempotent
/// calls doesn't break the check — `has_write_id` scans all snapshots, so
/// the duplicate id must still be found even when it's no longer the tip.
#[tokio::test]
async fn write_idempotent_skips_duplicate_id_across_instances() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

    let rows = vec![Person {
        name: "Alice".to_string(),
        age: 30,
        inserted: utc_now(),
    }];

    // First writer commits with "dup-id".
    let writer_1 = harness.get_table_writer::<Person>("people").await?;
    writer_1.write_idempotent("dup-id", rows.clone()).await?;
    drop(writer_1);

    // Interleave an unrelated plain write so the "dup-id" snapshot is no
    // longer main's tip. This ensures `has_write_id` has to scan snapshot
    // history, not just the current snapshot.
    let writer_mid = harness.get_table_writer::<Person>("people").await?;
    let bob = Person {
        name: "Bob".to_string(),
        age: 40,
        inserted: utc_now(),
    };
    writer_mid.write(vec![bob.clone()]).await?;
    drop(writer_mid);

    // Fresh writer — new `IcebergTable`, fresh metadata cache. The id
    // check must go to the catalog, not rely on in-memory state.
    let writer_2 = harness.get_table_writer::<Person>("people").await?;
    writer_2.write_idempotent("dup-id", rows.clone()).await?;

    // Exactly the rows from the first call plus Bob — Alice must not be
    // duplicated.
    let mut expected = rows.clone();
    expected.push(bob);
    expected.sort_by(|x, y| x.name.cmp(&y.name));
    let queried = harness
        .trino()
        .get_all::<Person>("SELECT * FROM default.people ORDER BY name".to_string())
        .await?
        .into_vec();
    assert_eq!(queried, expected);

    // Exactly one snapshot should carry the write_id in its summary.
    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    struct Count {
        c: i64,
    }
    // Trino `map[key]` raises on missing keys; `element_at` returns NULL
    // instead, which is what we want for snapshots that were plain `write`
    // calls (no write_id stamped).
    let counts = harness
        .trino()
        .get_all::<Count>(
            "SELECT count(*) AS c FROM default.\"people$snapshots\"
             WHERE element_at(summary, 'helium.write_id') = 'dup-id'"
                .to_string(),
        )
        .await?
        .into_vec();
    assert_eq!(counts, vec![Count { c: 1 }]);

    Ok(())
}

/// Concurrent writes to the same table must compose via `fast_append`:
/// neither writer should clobber the other. This test exercises the
/// retry-on-conflict path inside iceberg-rust's `Transaction::commit`.
///
/// Uses a multi-threaded runtime, two independent `IcebergTable` instances
/// (so each has its own metadata cache), and a barrier to force both
/// writers to reach their commit call simultaneously. One will win the
/// `RefSnapshotIdMatch` optimistic lock, the other gets a 409 and its
/// `do_commit` refreshes and re-applies fast_append against the new main.
///
/// Repeats the race several times to shake out scheduling-dependent flakes
/// — if any iteration lost a row, the assertion fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writes_dont_clobber() -> anyhow::Result<()> {
    const ROUNDS: usize = 5;

    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

    // Seed main so the table has a baseline snapshot.
    let seed_writer = harness.get_table_writer::<Person>("people").await?;
    let seed = Person {
        name: "Seed".to_string(),
        age: 1,
        inserted: utc_now(),
    };
    seed_writer.write(vec![seed.clone()]).await?;

    let mut expected: Vec<Person> = vec![seed];

    for round in 0..ROUNDS {
        // Two independent writer instances — each calls from_catalog, so
        // each has its own Arc<RwLock<Table>> metadata cache. This is the
        // critical difference from cloning a single BoxedDataWriter.
        let writer_a: Arc<dyn DataWriter<Person>> =
            harness.get_table_writer::<Person>("people").await?;
        let writer_b: Arc<dyn DataWriter<Person>> =
            harness.get_table_writer::<Person>("people").await?;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let row_a = Person {
            name: format!("Alice-{round}"),
            age: 30,
            inserted: utc_now(),
        };
        let row_b = Person {
            name: format!("Bob-{round}"),
            age: 40,
            inserted: utc_now(),
        };

        let barrier_a = Arc::clone(&barrier);
        let row_a_clone = row_a.clone();
        let task_a = tokio::spawn(async move {
            // Wait until both tasks are ready so they hit the commit path
            // as close to simultaneously as possible.
            barrier_a.wait().await;
            writer_a
                .write_idempotent(&format!("a-{round}"), vec![row_a_clone])
                .await
        });

        let barrier_b = Arc::clone(&barrier);
        let row_b_clone = row_b.clone();
        let task_b = tokio::spawn(async move {
            barrier_b.wait().await;
            writer_b
                .write_idempotent(&format!("b-{round}"), vec![row_b_clone])
                .await
        });

        task_a.await??;
        task_b.await??;

        expected.push(row_a);
        expected.push(row_b);
    }

    expected.sort_by(|x, y| x.name.cmp(&y.name));
    let queried = harness
        .trino()
        .get_all::<Person>("SELECT * FROM default.people ORDER BY name".to_string())
        .await?
        .into_vec();
    assert_eq!(queried, expected);

    Ok(())
}

/// A Trino DELETE that commits between two idempotent writes must be
/// preserved — this is the end-to-end reproduction of the bug where a
/// DELETE on `mobile.data_transfer.session` "came back" after a WAP
/// publish under the old branch-based workflow.
#[tokio::test]
async fn delete_between_writes_is_preserved() -> anyhow::Result<()> {
    let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;
    let writer = harness.get_table_writer::<Person>("people").await?;

    let alice = Person {
        name: "Alice".to_string(),
        age: 30,
        inserted: utc_now(),
    };
    let bob = Person {
        name: "Bob".to_string(),
        age: 40,
        inserted: utc_now(),
    };

    writer.write_idempotent("a", vec![alice.clone()]).await?;

    // External DELETE lands on main between our two writes.
    harness
        .trino()
        .execute("DELETE FROM default.people WHERE name = 'Alice'".to_string())
        .await?;

    writer.write_idempotent("b", vec![bob.clone()]).await?;

    let queried = harness
        .trino()
        .get_all::<Person>("SELECT * FROM default.people ORDER BY name".to_string())
        .await?
        .into_vec();
    assert_eq!(queried, vec![bob]);

    Ok(())
}
