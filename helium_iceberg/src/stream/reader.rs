//! Reading exactly the rows a single snapshot *added* to a table.
//!
//! `table.scan().snapshot_id(id)` scans a snapshot's full materialized state,
//! which can't isolate newly appended rows. So we drop to the manifest layer:
//! a snapshot's manifest list points at manifest files; the manifest entries
//! that carry `status == Added` and `snapshot_id == <this snapshot>` are
//! precisely the data files this snapshot appended. We hand-build a
//! `FileScanTask` per such entry and feed them to the same `ArrowReader` the
//! planner uses — reusing the reader without the (full-state) planner.
//!
//! All the iceberg APIs used here are public in iceberg-rust 0.9.

use crate::{Error, Result};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask};
use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};
use iceberg::table::Table;

/// Build a stream of Arrow `RecordBatch`es containing only the rows added by
/// `snapshot_id`. Returns an empty stream for a snapshot that added no data
/// files (e.g. an empty commit).
pub async fn added_data_files_to_batches(
    table: &Table,
    snapshot_id: i64,
) -> Result<ArrowRecordBatchStream> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or(Error::SnapshotNotFound { snapshot_id })?;

    // Read each added data file as the schema in effect at that snapshot, so
    // schema evolution between snapshots is handled per-snapshot rather than
    // forced onto the current schema.
    let schema = snapshot
        .schema_id()
        .and_then(|id| metadata.schema_by_id(id))
        .unwrap_or_else(|| metadata.current_schema())
        .clone();
    let project_field_ids = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<i32>>();

    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(Error::Iceberg)?;

    let mut tasks = Vec::new();
    for manifest_file in manifest_list.entries() {
        // Only data manifests, and only those introduced by this snapshot —
        // older manifests carried forward in this snapshot's list hold no
        // entries added by it. (The per-entry filter below is the authority;
        // this is the cheap pre-filter that avoids loading those manifests.)
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != snapshot_id
        {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(Error::Iceberg)?;

        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Added
                || entry.snapshot_id() != Some(snapshot_id)
                || entry.content_type() != DataContentType::Data
            {
                continue;
            }

            tasks.push(FileScanTask {
                file_size_in_bytes: entry.file_size_in_bytes(),
                start: 0,
                length: entry.file_size_in_bytes(),
                record_count: Some(entry.record_count()),
                data_file_path: entry.file_path().to_string(),
                data_file_format: entry.file_format(),
                schema: schema.clone(),
                project_field_ids: project_field_ids.clone(),
                predicate: None,
                deletes: vec![],
                partition: Some(entry.data_file().partition().clone()),
                partition_spec: None,
                name_mapping: None,
                case_sensitive: true,
            });
        }
    }

    let task_stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    ArrowReaderBuilder::new(file_io.clone())
        .build()
        .read(task_stream)
        .map_err(Error::Iceberg)
}

/// Collect all added rows for a snapshot into a single concatenated set of
/// batches. Convenience over [`added_data_files_to_batches`] for callers that
/// want the whole snapshot's additions in memory.
pub async fn added_record_batches(
    table: &Table,
    snapshot_id: i64,
) -> Result<Vec<arrow_array::RecordBatch>> {
    added_data_files_to_batches(table, snapshot_id)
        .await?
        .map_err(Error::Iceberg)
        .try_collect()
        .await
}
