//! Offset/watermark persistence for the Iceberg snapshot stream poller.
//!
//! This mirrors `file_store`'s `FileInfoPollerState` / `FileInfoPollerStateRecorder`
//! split, but the unit of progress is an Iceberg *snapshot* rather than an S3
//! file. Because an Iceberg table's snapshot log is strictly ordered by
//! `sequence_number`, a single high-watermark per `(process_name, table_name)`
//! is sufficient — there is no out-of-order arrival to guard against, so none
//! of the file poller's `exists`/`clean`/cache machinery is needed here.
//!
//! Two SQL backends are provided, both behind features and both preserving the
//! same flow: `record` writes into a transaction the consumer commits at the
//! end (so the watermark advance is atomic and durable, and a crash before
//! commit re-runs the snapshot — at-least-once).
//!
//! - `sqlite` (recommended default): a single embedded db file per poller on a
//!   PVC, opened via [`open_watermark_db`], which applies the bundled migration
//!   creating `iceberg_snapshots_processed`. No external database server.
//! - `sqlx-postgres`: impls on `sqlx::Pool<Postgres>` / `sqlx::Transaction` for
//!   consumers that want to co-commit the watermark with their own Postgres
//!   work. The table is owned by the *consuming* crate's migrations, exactly
//!   like `files_processed`.

use crate::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::spec::Operation;

/// Metadata describing a single Iceberg snapshot consumed as a stream event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub timestamp: DateTime<Utc>,
    pub operation: Operation,
}

/// Reads the persisted watermark for a `(process_name, table_name)` stream.
///
/// The poller seeds its in-memory watermark from `latest_sequence_number` at
/// startup and only emits snapshots whose `sequence_number` is strictly
/// greater than it.
#[async_trait]
pub trait IcebergStreamState: Send + Sync + 'static {
    /// The highest `sequence_number` recorded as processed for this stream, or
    /// `None` if nothing has been processed yet.
    async fn latest_sequence_number(
        &self,
        process_name: &str,
        table_name: &str,
    ) -> Result<Option<i64>>;
}

/// Records a snapshot as processed. Implemented for a transaction handle so a
/// consumer can co-commit the watermark advance with its own side effects,
/// giving at-least-once delivery (mirrors `FileInfoPollerStateRecorder`).
#[async_trait]
pub trait IcebergStreamStateRecorder {
    async fn record(
        &mut self,
        process_name: &str,
        table_name: &str,
        snapshot: &SnapshotMeta,
    ) -> Result<()>;
}

#[cfg(feature = "sqlx-postgres")]
mod sqlx_postgres {
    use super::{IcebergStreamState, IcebergStreamStateRecorder, SnapshotMeta};
    use crate::{Error, Result};
    use async_trait::async_trait;
    use chrono::Utc;

    #[async_trait]
    impl IcebergStreamState for sqlx::Pool<sqlx::Postgres> {
        async fn latest_sequence_number(
            &self,
            process_name: &str,
            table_name: &str,
        ) -> Result<Option<i64>> {
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(sequence_number) FROM iceberg_snapshots_processed
                WHERE process_name = $1 AND table_name = $2
                "#,
            )
            .bind(process_name)
            .bind(table_name)
            .fetch_one(self)
            .await
            .map_err(Error::from)
        }
    }

    #[async_trait]
    impl IcebergStreamStateRecorder for sqlx::Transaction<'_, sqlx::Postgres> {
        async fn record(
            &mut self,
            process_name: &str,
            table_name: &str,
            snapshot: &SnapshotMeta,
        ) -> Result<()> {
            sqlx::query(
                r#"
                INSERT INTO iceberg_snapshots_processed
                    (process_name, table_name, snapshot_id, sequence_number, snapshot_timestamp, processed_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (process_name, table_name, sequence_number) DO NOTHING
                "#,
            )
            .bind(process_name)
            .bind(table_name)
            .bind(snapshot.snapshot_id)
            .bind(snapshot.sequence_number)
            .bind(snapshot.timestamp)
            .bind(Utc::now())
            .execute(&mut **self)
            .await
            .map(|_| ())
            .map_err(Error::from)
        }
    }
}

#[cfg(feature = "sqlite")]
pub use sqlite::open_watermark_db;

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::{IcebergStreamState, IcebergStreamStateRecorder, SnapshotMeta};
    use crate::{Error, Result};
    use async_trait::async_trait;
    use chrono::Utc;
    use sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };
    use sqlx::SqlitePool;
    use std::path::Path;
    use std::time::Duration;

    /// Watermark-table migration, bundled in this crate (`./migrations`).
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

    /// Open (creating if absent) the per-poller SQLite watermark database at
    /// `path`, apply the bundled migration, and return the pool. Use one file
    /// per poller; pass the returned pool to both `.state(...)` and the
    /// consumer's `pool.begin()` for the recording transaction.
    pub async fn open_watermark_db(path: impl AsRef<Path>) -> Result<SqlitePool> {
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

        MIGRATOR.run(&pool).await?;
        Ok(pool)
    }

    #[async_trait]
    impl IcebergStreamState for SqlitePool {
        async fn latest_sequence_number(
            &self,
            process_name: &str,
            table_name: &str,
        ) -> Result<Option<i64>> {
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(sequence_number) FROM iceberg_snapshots_processed
                WHERE process_name = ? AND table_name = ?
                "#,
            )
            .bind(process_name)
            .bind(table_name)
            .fetch_one(self)
            .await
            .map_err(Error::from)
        }
    }

    #[async_trait]
    impl IcebergStreamStateRecorder for sqlx::Transaction<'_, sqlx::Sqlite> {
        async fn record(
            &mut self,
            process_name: &str,
            table_name: &str,
            snapshot: &SnapshotMeta,
        ) -> Result<()> {
            sqlx::query(
                r#"
                INSERT INTO iceberg_snapshots_processed
                    (process_name, table_name, snapshot_id, sequence_number, snapshot_timestamp, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (process_name, table_name, sequence_number) DO NOTHING
                "#,
            )
            .bind(process_name)
            .bind(table_name)
            .bind(snapshot.snapshot_id)
            .bind(snapshot.sequence_number)
            .bind(snapshot.timestamp)
            .bind(Utc::now())
            .execute(&mut **self)
            .await
            .map(|_| ())
            .map_err(Error::from)
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use super::{open_watermark_db, IcebergStreamState, IcebergStreamStateRecorder, SnapshotMeta};
    use chrono::Utc;
    use iceberg::spec::Operation;

    fn meta(sequence_number: i64) -> SnapshotMeta {
        SnapshotMeta {
            snapshot_id: sequence_number * 10,
            sequence_number,
            timestamp: Utc::now(),
            operation: Operation::Append,
        }
    }

    #[tokio::test]
    async fn watermark_roundtrips_and_honors_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("watermark.db");

        let pool = open_watermark_db(&db).await.unwrap();
        assert_eq!(
            pool.latest_sequence_number("default", "t").await.unwrap(),
            None
        );

        // A committed record advances the watermark.
        let mut txn = pool.begin().await.unwrap();
        txn.record("default", "t", &meta(5)).await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(
            pool.latest_sequence_number("default", "t").await.unwrap(),
            Some(5)
        );

        // A rolled-back record does NOT — the at-least-once property.
        let mut txn = pool.begin().await.unwrap();
        txn.record("default", "t", &meta(6)).await.unwrap();
        txn.rollback().await.unwrap();
        assert_eq!(
            pool.latest_sequence_number("default", "t").await.unwrap(),
            Some(5)
        );

        // Re-opening the same file keeps the watermark (PVC persistence).
        drop(pool);
        let pool = open_watermark_db(&db).await.unwrap();
        assert_eq!(
            pool.latest_sequence_number("default", "t").await.unwrap(),
            Some(5)
        );
    }
}
