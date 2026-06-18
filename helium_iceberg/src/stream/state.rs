//! Per-poller SQLite watermark store for the Iceberg snapshot stream poller.
//!
//! The watermark is a single high-`sequence_number` per
//! `(process_name, table_name)` — Iceberg's snapshot log is strictly ordered,
//! so one watermark per stream is sufficient (no out-of-order arrival to guard
//! against). It lives in a small SQLite db: one file per poller, mount it on a
//! PVC to persist progress. The watermark table is created by the bundled
//! migration when the store is opened.
//!
//! A [`StreamState`] is scoped to one `(process_name, table_name)`, so those
//! identifiers live on the store rather than being threaded through every call.
//!
//! Reading and recording are decoupled: the poller seeds its in-memory
//! watermark from [`StreamState::latest_sequence_number`] at startup, and the
//! consumer calls [`record`](StreamState::record) (via
//! [`IcebergEvent::commit`](super::IcebergEvent::commit)) *after* its sink write
//! has committed. A crash before recording re-runs the snapshot —
//! at-least-once, made safe by an idempotent sink.

use crate::{Error, Result};
use chrono::{DateTime, Utc};
use iceberg::spec::Operation;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use std::path::Path;
use std::time::Duration;

/// Metadata describing a single Iceberg snapshot consumed as a stream event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub timestamp: DateTime<Utc>,
    pub operation: Operation,
}

/// Watermark-table migration bundled in this crate (`./migrations`).
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// SQLite-backed watermark store, scoped to one `(process_name, table_name)` —
/// one db file per poller. Cheap to clone (`SqlitePool` is reference-counted);
/// the poller stamps a clone into every emitted event so the consumer can
/// record against the same db.
///
/// Internal: callers configure the poller with a `db_dir` or a pre-created
/// `sqlx::SqlitePool`, never this wrapper directly.
#[derive(Debug, Clone)]
pub(crate) struct StreamState {
    pool: SqlitePool,
    process_name: String,
    table_name: String,
}

impl StreamState {
    /// Open (creating if absent) the watermark db at `path` for the given
    /// stream identity and apply the bundled migration.
    pub(crate) async fn open(
        path: impl AsRef<Path>,
        process_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            // WAL + NORMAL is the standard durable-yet-fast combo; commits fsync.
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        // One writer per poller — a single connection serializes the (rare)
        // writes and keeps WAL handling simple.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        Self::from_pool(pool, process_name, table_name).await
    }

    /// Wrap a caller-provided pool for the given stream identity, applying the
    /// bundled migration so the watermark table exists.
    pub(crate) async fn from_pool(
        pool: SqlitePool,
        process_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            process_name: process_name.into(),
            table_name: table_name.into(),
        })
    }

    pub(crate) fn process_name(&self) -> &str {
        &self.process_name
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    /// The highest `sequence_number` recorded as processed for this stream, or
    /// `None` if nothing has been processed yet.
    pub(crate) async fn latest_sequence_number(&self) -> Result<Option<i64>> {
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MAX(sequence_number) FROM iceberg_snapshots_processed
            WHERE process_name = ? AND table_name = ?
            "#,
        )
        .bind(&self.process_name)
        .bind(&self.table_name)
        .fetch_one(&self.pool)
        .await
        .map_err(Error::from)
    }

    /// Record a snapshot as processed, advancing the durable watermark. A
    /// single autocommitting statement, idempotent on the snapshot key, so a
    /// reprocessed snapshot (after a crash before recording) is a no-op.
    pub(crate) async fn record(&self, snapshot: &SnapshotMeta) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO iceberg_snapshots_processed
                (process_name, table_name, snapshot_id, sequence_number, snapshot_timestamp, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (process_name, table_name, sequence_number) DO NOTHING
            "#,
        )
        .bind(&self.process_name)
        .bind(&self.table_name)
        .bind(snapshot.snapshot_id)
        .bind(snapshot.sequence_number)
        .bind(snapshot.timestamp)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(sequence_number: i64) -> SnapshotMeta {
        SnapshotMeta {
            snapshot_id: sequence_number * 10,
            sequence_number,
            timestamp: Utc::now(),
            operation: Operation::Append,
        }
    }

    #[tokio::test]
    async fn watermark_roundtrips_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("watermark.db");

        let store = StreamState::open(&db, "default", "t").await.unwrap();
        assert_eq!(store.latest_sequence_number().await.unwrap(), None);

        // record advances the watermark
        store.record(&meta(5)).await.unwrap();
        assert_eq!(store.latest_sequence_number().await.unwrap(), Some(5));

        // higher sequence numbers advance it; the query is MAX, so a lower or
        // duplicate sequence number never lowers it
        store.record(&meta(6)).await.unwrap();
        store.record(&meta(5)).await.unwrap(); // idempotent no-op
        assert_eq!(store.latest_sequence_number().await.unwrap(), Some(6));

        // re-opening the same file keeps the watermark (PVC persistence)
        drop(store);
        let store = StreamState::open(&db, "default", "t").await.unwrap();
        assert_eq!(store.latest_sequence_number().await.unwrap(), Some(6));
    }
}
