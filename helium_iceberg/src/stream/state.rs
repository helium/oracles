//! Offset/watermark persistence for the Iceberg snapshot stream poller.
//!
//! This mirrors `file_store`'s `FileInfoPollerState` / `FileInfoPollerStateRecorder`
//! split, but the unit of progress is an Iceberg *snapshot* rather than an S3
//! file. Because an Iceberg table's snapshot log is strictly ordered by
//! `sequence_number`, a single high-watermark per `(process_name, table_name)`
//! is sufficient — there is no out-of-order arrival to guard against, so none
//! of the file poller's `exists`/`clean`/cache machinery is needed here.
//!
//! The `sqlx::Pool<Postgres>` / `sqlx::Transaction` impls live behind the
//! `sqlx-postgres` feature so write-only users of this crate don't pull in
//! sqlx. The table they expect (owned by the *consuming* crate's migrations,
//! exactly like `files_processed`):
//!
//! ```sql
//! CREATE TABLE iceberg_snapshots_processed (
//!     process_name       TEXT        NOT NULL DEFAULT 'default',
//!     table_name         TEXT        NOT NULL,
//!     snapshot_id        BIGINT      NOT NULL,
//!     sequence_number    BIGINT      NOT NULL,
//!     snapshot_timestamp TIMESTAMPTZ NOT NULL,
//!     processed_at       TIMESTAMPTZ NOT NULL,
//!     PRIMARY KEY (process_name, table_name, sequence_number)
//! );
//! ```

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

    impl From<sqlx::Error> for Error {
        fn from(err: sqlx::Error) -> Self {
            Error::State(err.to_string())
        }
    }

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
