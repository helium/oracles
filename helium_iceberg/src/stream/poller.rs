//! The snapshot stream poller — the Iceberg counterpart to
//! `file_store`'s `FileInfoPollerServer`.
//!
//! It polls a table's snapshot log on an interval and emits each new snapshot
//! (since the persisted watermark) as an [`IcebergStream`] over an mpsc
//! channel, with the rows that snapshot appended already decoded. It is a
//! [`ManagedTask`], so it slots into a `task_manager::TaskManager` alongside a
//! `task_manager::channel_consumer` exactly like the file poller does.
//!
//! Progress is persisted to a per-poller SQLite [`WatermarkStore`]: either one
//! the poller opens under a caller-provided directory
//! (`{namespace}-{table}-{process_name}.db`) or a pre-created store passed via
//! `.store(...)`.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::Instant;

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures::TryFutureExt;
use iceberg::spec::Operation;
use iceberg::table::Table;
use serde::de::DeserializeOwned;
use sqlx::SqlitePool;
use task_manager::ManagedTask;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::catalog::Catalog;
use crate::stream::parser::batch_to_records;
use crate::stream::reader::added_record_batches;
use crate::stream::state::{SnapshotMeta, StreamState};
use crate::stream::IcebergEvent;
use crate::{Error, Result};

const DEFAULT_POLL_DURATION: std::time::Duration = std::time::Duration::from_secs(30);
const DEFAULT_QUEUE_SIZE: usize = 5;

/// Env var supplying the default watermark-db directory when `db_dir` isn't set
/// explicitly (and no `pool` is provided).
const DB_DIR_ENV: &str = "ICEBERG_STREAM_DB_DIR";

/// Default `db_dir`: `$ICEBERG_STREAM_DB_DIR`, or the current directory.
fn default_db_dir() -> PathBuf {
    std::env::var_os(DB_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Counter incremented by the number of snapshots that were expired before the
/// poller could process them (i.e. permanently-missed appended rows).
const EXPIRED_SNAPSHOTS_SKIPPED_METRIC: &str = "iceberg-stream-expired-snapshots-skipped";

/// Histogram of catalog `load_table` latency, in seconds. Its exported
/// `_count` doubles as "how often the poller hits the catalog" (every poll
/// tick, including idle ticks and retries), so no separate counter is needed.
const LOAD_TABLE_DURATION_METRIC: &str = "iceberg-stream-load-table-duration-seconds";

/// Where to begin reading when there is no persisted watermark yet.
///
/// Snapshots older than the resulting cutoff timestamp are never emitted.
#[derive(Debug, Clone)]
pub enum LookbackBehavior {
    /// Only emit snapshots committed strictly after this instant.
    StartAfter(DateTime<Utc>),
    /// Only emit snapshots committed within `now - max_lookback`.
    Max(std::time::Duration),
}

impl Default for LookbackBehavior {
    fn default() -> Self {
        // Can only go back to the oldest snapshot still available on the table.
        LookbackBehavior::StartAfter(DateTime::<Utc>::UNIX_EPOCH)
    }
}

impl From<DateTime<Utc>> for LookbackBehavior {
    fn from(value: DateTime<Utc>) -> Self {
        LookbackBehavior::StartAfter(value)
    }
}

impl From<std::time::Duration> for LookbackBehavior {
    fn from(value: std::time::Duration) -> Self {
        LookbackBehavior::Max(value)
    }
}

impl LookbackBehavior {
    fn cutoff(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            LookbackBehavior::StartAfter(start_after) => *start_after,
            LookbackBehavior::Max(max_lookback) => now - *max_lookback,
        }
    }
}

#[derive(Clone, Builder)]
#[builder(pattern = "owned", build_fn(error = "crate::Error"))]
pub struct IcebergStreamPollerConfig<Message> {
    catalog: Catalog,
    #[builder(setter(into))]
    namespace: String,
    #[builder(setter(into, name = "table"))]
    table_name: String,
    /// Pre-created SQLite pool for the watermark store. When set, the poller
    /// migrates and uses it, and [`db_dir`](Self::db_dir) is ignored. When
    /// unset, the poller opens a db under `db_dir`.
    #[builder(default, setter(strip_option))]
    pool: Option<SqlitePool>,
    /// Directory the poller opens the watermark db in (as
    /// `{namespace}-{table}-{process_name}.db`) when no `pool` is provided.
    /// Mount this on a PVC. Defaults to `$ICEBERG_STREAM_DB_DIR`, falling back
    /// to the current directory.
    #[builder(default = "default_db_dir()", setter(into))]
    db_dir: PathBuf,
    #[builder(default = "DEFAULT_POLL_DURATION")]
    poll_duration: std::time::Duration,
    #[builder(default = "DEFAULT_QUEUE_SIZE")]
    queue_size: usize,
    #[builder(default = r#""default".to_string()"#)]
    process_name: String,
    #[builder(default)]
    lookback: LookbackBehavior,
    /// Skip (rather than error on) non-append snapshots (overwrite/replace/delete),
    /// mirroring Spark's `streaming-skip-overwrite-snapshots` /
    /// `streaming-skip-delete-snapshots`. Defaults to true.
    #[builder(default = "true")]
    skip_non_append: bool,
    #[builder(default, setter(custom))]
    stop_after: Option<DateTime<Utc>>,
    #[builder(default, setter(custom))]
    idle_timeout: Option<std::time::Duration>,
    #[builder(setter(skip))]
    p: PhantomData<Message>,
}

impl<Message> IcebergStreamPollerConfigBuilder<Message> {
    /// Begin reading after the given commit time (only used until a watermark exists).
    pub fn lookback_start_after(self, start_after: DateTime<Utc>) -> Self {
        self.lookback(LookbackBehavior::StartAfter(start_after))
    }

    /// Begin reading from within `now - max_lookback`.
    pub fn lookback_max(self, max_lookback: std::time::Duration) -> Self {
        self.lookback(LookbackBehavior::Max(max_lookback))
    }

    /// Stop once a snapshot committed after `stop_after` is encountered (the
    /// boundary snapshot is not emitted), for bounded catch-up jobs.
    pub fn stop_after(mut self, stop_after: DateTime<Utc>) -> Self {
        self.stop_after = Some(Some(stop_after));
        self
    }

    /// Stop at the current time. Equivalent to `.stop_after(Utc::now())`.
    pub fn stop_at_now(self) -> Self {
        self.stop_after(Utc::now())
    }

    /// Run continuously (default).
    pub fn stop_never(mut self) -> Self {
        self.stop_after = Some(None);
        self
    }

    /// Exit cleanly after being idle (no new snapshots) for `timeout`.
    pub fn idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_timeout = Some(Some(timeout));
        self
    }

    /// Wait indefinitely for new snapshots (default).
    pub fn no_idle_timeout(mut self) -> Self {
        self.idle_timeout = Some(None);
        self
    }

    /// Set the idle timeout from an optional value.
    pub fn idle_timeout_opt(self, idle_timeout: Option<std::time::Duration>) -> Self {
        match idle_timeout {
            Some(timeout) => self.idle_timeout(timeout),
            None => self.no_idle_timeout(),
        }
    }

    /// Set the poll duration from an optional value (falls back to the default).
    pub fn poll_duration_opt(self, poll_duration: Option<std::time::Duration>) -> Self {
        self.poll_duration(poll_duration.unwrap_or(DEFAULT_POLL_DURATION))
    }

    /// Build the poller, returning the receiver and the server task. If no
    /// `.pool(...)` was provided, opens (and migrates) a per-poller SQLite db
    /// at `{db_dir}/{namespace}-{table}-{process_name}.db`.
    pub async fn create(
        self,
    ) -> Result<(
        Receiver<IcebergEvent<Message>>,
        IcebergStreamPollerServer<Message>,
    )> {
        let mut config = self.build()?;
        let store = match config.pool.take() {
            Some(pool) => {
                StreamState::from_pool(pool, &config.process_name, &config.table_name).await?
            }
            None => {
                let path = config.db_dir.join(format!(
                    "{}-{}-{}.db",
                    config.namespace, config.table_name, config.process_name
                ));
                StreamState::open(path, &config.process_name, &config.table_name).await?
            }
        };
        let latest_sequence_number = store.latest_sequence_number().await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(config.queue_size);

        Ok((
            receiver,
            IcebergStreamPollerServer {
                config,
                store,
                sender,
                latest_sequence_number,
                idle_since: None,
                warned_gap_at: None,
            },
        ))
    }
}

pub struct IcebergStreamPollerServer<Message> {
    config: IcebergStreamPollerConfig<Message>,
    store: StreamState,
    sender: Sender<IcebergEvent<Message>>,
    latest_sequence_number: Option<i64>,
    idle_since: Option<Instant>,
    /// Watermark value we last emitted an expired-gap warning for, so we don't
    /// re-warn on every poll while sitting at the same watermark.
    warned_gap_at: Option<i64>,
}

/// A run of snapshots that were expired before the poller could process them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpiredGap {
    /// The watermark we resumed from.
    watermark: i64,
    /// The lowest sequence number still present above the watermark.
    next_available_sequence_number: i64,
    /// How many snapshots between them are gone.
    skipped: i64,
}

/// Detect snapshots that were expired before being processed.
///
/// With a seeded watermark `w`, the next snapshot still in the table should be
/// `w + 1` (Iceberg v2 sequence numbers are contiguous). If the lowest one
/// present above the watermark is higher than that, the snapshots in between
/// were expired — their appended rows can never be streamed incrementally.
///
/// Returns `None` on cold start (no watermark), when caught up, or when the
/// next snapshot is exactly `w + 1`.
fn detect_expired_gap(
    watermark: Option<i64>,
    sequence_numbers: impl IntoIterator<Item = i64>,
) -> Option<ExpiredGap> {
    let watermark = watermark?;
    let next_available_sequence_number = sequence_numbers
        .into_iter()
        .filter(|seq| *seq > watermark)
        .min()?;
    (next_available_sequence_number > watermark + 1).then_some(ExpiredGap {
        watermark,
        next_available_sequence_number,
        skipped: next_available_sequence_number - watermark - 1,
    })
}

/// A snapshot selected for emission, together with the table it was read from.
struct NextSnapshot {
    table: Table,
    meta: SnapshotMeta,
}

impl<Message> ManagedTask for IcebergStreamPollerServer<Message>
where
    Message: DeserializeOwned + Send + Sync + 'static,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<Message> IcebergStreamPollerServer<Message>
where
    Message: DeserializeOwned + Send + Sync + 'static,
{
    /// Pick the lowest-`sequence_number` snapshot that is past both the
    /// watermark and the lookback cutoff. Returns the chosen snapshot's
    /// metadata, or `None` if there's nothing new.
    fn select_next(&self, table: &Table) -> Result<Option<SnapshotMeta>> {
        let cutoff = self.config.lookback.cutoff(Utc::now());
        let watermark = self.latest_sequence_number.unwrap_or(i64::MIN);

        // Single O(n) pass picking the lowest sequence number above the
        // watermark whose commit time clears the lookback cutoff. The snapshot
        // list isn't ordered, and `timestamp()` is fallible and must stay a
        // filter (a lower-sequence snapshot can fall below the cutoff while a
        // higher one clears it), so a min-scan beats sorting then taking the
        // first match. We carry the winning reference and build the
        // `SnapshotMeta` once at the end rather than per candidate.
        let mut best: Option<(&iceberg::spec::SnapshotRef, DateTime<Utc>)> = None;
        for snapshot in table.metadata().snapshots() {
            if snapshot.sequence_number() <= watermark {
                continue;
            }
            let timestamp = snapshot.timestamp().map_err(Error::Iceberg)?;
            if timestamp <= cutoff {
                continue;
            }
            if best.is_none_or(|(b, _)| snapshot.sequence_number() < b.sequence_number()) {
                best = Some((snapshot, timestamp));
            }
        }

        Ok(best.map(|(snapshot, timestamp)| SnapshotMeta {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            timestamp,
            operation: snapshot.summary().operation.clone(),
        }))
    }

    /// Warn (once per watermark) if snapshots were expired before we processed
    /// them. The stream resumes at the next available snapshot regardless —
    /// the gapped rows are simply never emitted — so this is observability, not
    /// a recoverable condition.
    fn warn_on_expired_gap(&mut self, table: &Table) {
        if self.warned_gap_at == self.latest_sequence_number {
            return;
        }
        let sequence_numbers = table
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.sequence_number());
        if let Some(gap) = detect_expired_gap(self.latest_sequence_number, sequence_numbers) {
            tracing::warn!(
                table = self.config.table_name,
                process_name = self.config.process_name,
                watermark = gap.watermark,
                next_available_sequence_number = gap.next_available_sequence_number,
                skipped_snapshots = gap.skipped,
                "snapshots expired before processing; their appended rows will not be streamed"
            );
            // A monotonic counter so the gap is alertable long after the log
            // line has scrolled out of retention — any nonzero increment means
            // appended rows were permanently missed for this table.
            metrics::counter!(
                EXPIRED_SNAPSHOTS_SKIPPED_METRIC,
                "table" => self.config.table_name.clone(),
                "process-name" => self.config.process_name.clone(),
            )
            .increment(gap.skipped as u64);
            self.warned_gap_at = self.latest_sequence_number;
        }
    }

    fn idle_timeout_elapsed(&self, idle_start: Instant) -> bool {
        let Some(idle_timeout) = self.config.idle_timeout else {
            return false;
        };
        if idle_start.elapsed() >= idle_timeout {
            tracing::info!(
                idle_duration = ?idle_start.elapsed(),
                ?idle_timeout,
                "idle timeout exceeded, closing iceberg stream"
            );
            true
        } else {
            false
        }
    }

    /// Reload the table from the catalog, recording the latency of the
    /// (network) `load_table` call — the histogram's `_count` also tells how
    /// often it's called. Latency is recorded for failures too, so a
    /// slow-then-erroring catalog still shows up.
    async fn load_table(&self) -> Result<Table> {
        let started = Instant::now();
        let result = self
            .config
            .catalog
            .load_table(&self.config.namespace, &self.config.table_name)
            .await;

        metrics::histogram!(
            LOAD_TABLE_DURATION_METRIC,
            "table" => self.config.table_name.clone(),
            "process-name" => self.config.process_name.clone(),
        )
        .record(started.elapsed().as_secs_f64());

        result
    }

    /// Resolve the next snapshot to emit, handling skipped non-append
    /// snapshots, `stop_after`, and idle/poll waiting internally. Returns
    /// `None` to close the stream (stop boundary reached or idle timeout).
    async fn get_next_snapshot(&mut self) -> Result<Option<NextSnapshot>> {
        loop {
            let table = self.load_table().await?;

            self.warn_on_expired_gap(&table);

            if let Some(meta) = self.select_next(&table)? {
                if let Some(stop_after) = self.config.stop_after {
                    if meta.timestamp > stop_after {
                        tracing::info!(
                            snapshot_id = meta.snapshot_id,
                            %stop_after,
                            "snapshot reached stop_after, closing iceberg stream"
                        );
                        return Ok(None);
                    }
                }

                if meta.operation != Operation::Append {
                    if self.config.skip_non_append {
                        tracing::info!(
                            snapshot_id = meta.snapshot_id,
                            operation = ?meta.operation,
                            "skipping non-append snapshot"
                        );
                        // Advance past it so we don't reconsider it this run.
                        self.latest_sequence_number = Some(meta.sequence_number);
                        self.idle_since = None;
                        continue;
                    }
                    return Err(Error::NonAppendSnapshot {
                        snapshot_id: meta.snapshot_id,
                        operation: meta.operation.clone(),
                    });
                }

                self.idle_since = None;
                return Ok(Some(NextSnapshot { table, meta }));
            }

            let idle_start = *self.idle_since.get_or_insert_with(Instant::now);
            if self.idle_timeout_elapsed(idle_start) {
                return Ok(None);
            }
            tokio::time::sleep(self.config.poll_duration).await;
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        let process_name = self.config.process_name.clone();
        let table_name = self.config.table_name.clone();

        tracing::info!(
            namespace = self.config.namespace,
            table = table_name,
            %process_name,
            stop_after = ?self.config.stop_after,
            idle_timeout = ?self.config.idle_timeout,
            "starting IcebergStreamPoller",
        );

        let sender = self.sender.clone();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!(table = table_name, %process_name, "stopping IcebergStreamPoller");
                    break;
                }
                result = futures::future::try_join(
                    sender.reserve().map_err(|_| Error::ConsumerDropped {
                        table: table_name.clone(),
                        process_name: process_name.clone(),
                    }),
                    self.get_next_snapshot(),
                ) => {
                    match result? {
                        (permit, Some(next)) => {
                            let batches = added_record_batches(&next.table, next.meta.snapshot_id).await?;
                            let mut data = Vec::new();
                            for batch in batches {
                                data.extend(batch_to_records::<Message>(&batch)?);
                            }

                            let sequence_number = next.meta.sequence_number;
                            let stream = IcebergEvent::new(self.store.clone(), next.meta, data);
                            permit.send(stream);
                            self.latest_sequence_number = Some(sequence_number);
                        }
                        (_, None) => {
                            tracing::info!(table = table_name, %process_name, "IcebergStreamPoller completed");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{detect_expired_gap, ExpiredGap};

    #[test]
    fn cold_start_never_reports_a_gap() {
        assert_eq!(detect_expired_gap(None, [1, 2, 3]), None);
    }

    #[test]
    fn contiguous_resume_is_not_a_gap() {
        // Watermark 3, next available is exactly 4.
        assert_eq!(detect_expired_gap(Some(3), [4, 5, 6]), None);
    }

    #[test]
    fn caught_up_is_not_a_gap() {
        // Nothing above the watermark.
        assert_eq!(detect_expired_gap(Some(7), [5, 6, 7]), None);
    }

    #[test]
    fn missing_successor_is_a_gap() {
        // Watermark 3, but 4/5/6 were expired; next present is 7.
        assert_eq!(
            detect_expired_gap(Some(3), [7, 8, 9]),
            Some(ExpiredGap {
                watermark: 3,
                next_available_sequence_number: 7,
                skipped: 3,
            })
        );
    }

    #[test]
    fn gap_uses_lowest_present_above_watermark() {
        // Order shouldn't matter; the relevant snapshot is the min above 3.
        assert_eq!(
            detect_expired_gap(Some(3), [9, 5, 8]),
            Some(ExpiredGap {
                watermark: 3,
                next_available_sequence_number: 5,
                skipped: 1,
            })
        );
    }
}
