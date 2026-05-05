//! On-disk spool for `BatchedWriter`.
//!
//! Records arriving via `BatchedWriter::queue` are converted to
//! `RecordBatch` and appended to a per-task Arrow-IPC stream file in
//! `spool_dir`. The buffer lives on disk, not in memory, so a crash
//! between queue and flush is recoverable: on the next startup,
//! `Spool::replay_dir` reads any leftover files for the same table and
//! commits them to Iceberg before the writer accepts new work.
//!
//! The IPC stream format is chosen because each batch is length-prefixed
//! — a partial trailing batch from a hard kill is detectable on read and
//! discarded, so the well-formed prefix still replays cleanly.
//!
//! Files are opened lazily: a fresh `Spool` and a `Spool` immediately
//! after a successful flush both have no open file. The next `append`
//! opens one. This keeps clean shutdowns from leaving zero-record files
//! behind in the spool directory.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::SchemaRef;
use iceberg::arrow::schema_to_arrow_schema;
use serde::Serialize;
use uuid::Uuid;

use crate::iceberg_table::IcebergTable;
use crate::{Error, Result};

const SPOOL_FILE_EXT: &str = "arrow";

pub(super) struct Spool {
    spool_dir: PathBuf,
    table_prefix: String,
    schema: SchemaRef,
    /// `None` until the first append after construction or after a
    /// successful flush.
    open: Option<OpenFile>,
}

struct OpenFile {
    path: PathBuf,
    writer: StreamWriter<BufWriter<File>>,
    record_count: usize,
}

impl Spool {
    /// Construct a Spool bound to `table` in `spool_dir`. Doesn't open
    /// a file yet — the first `append` does.
    pub(super) async fn create<T>(spool_dir: &Path, table: &IcebergTable<T>) -> Result<Self>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let schema = arrow_schema_for(table).await?;
        let table_prefix = table_prefix_for(table).await?;

        tokio::fs::create_dir_all(spool_dir)
            .await
            .map_err(|e| Error::Writer(format!("create spool dir: {e}")))?;

        Ok(Self {
            spool_dir: spool_dir.to_path_buf(),
            table_prefix,
            schema,
            open: None,
        })
    }

    pub(super) fn record_count(&self) -> usize {
        self.open.as_ref().map(|o| o.record_count).unwrap_or(0)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.record_count() == 0
    }

    /// Append a `RecordBatch` and push it through the BufWriter so it
    /// reaches the kernel page cache. Doesn't fsync — see
    /// `flush_to_iceberg` for that. Pushing past BufWriter on every
    /// append makes the batch recoverable across a process abort, even
    /// without an explicit shutdown.
    pub(super) async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let added = batch.num_rows();

        let mut open = match self.open.take() {
            Some(o) => o,
            None => {
                let (path, writer) =
                    open_new_writer(&self.spool_dir, &self.table_prefix, self.schema.clone())
                        .await?;
                OpenFile {
                    path,
                    writer,
                    record_count: 0,
                }
            }
        };

        let writer = open.writer;
        let (writer, write_res) =
            tokio::task::spawn_blocking(move || -> (_, std::io::Result<()>) {
                let mut writer = writer;
                let res = (|| {
                    writer
                        .write(&batch)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    writer.get_mut().flush()
                })();
                (writer, res)
            })
            .await
            .map_err(|e| Error::Writer(format!("join error: {e}")))?;

        open.writer = writer;
        write_res.map_err(|e| Error::Writer(format!("spool append: {e}")))?;
        open.record_count += added;
        self.open = Some(open);
        Ok(())
    }

    /// Close the current writer, fsync, read all batches back, commit
    /// them as a single Iceberg snapshot, and delete the file. Leaves
    /// the spool with no open file — the next append opens a fresh one.
    /// No-op if no file is open.
    pub(super) async fn flush_to_iceberg<T>(&mut self, table: &IcebergTable<T>) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let OpenFile {
            path,
            writer,
            record_count,
        } = open;

        if record_count == 0 {
            // Empty open file (shouldn't normally happen — append only
            // creates a file when there are rows to write — but guard
            // anyway). Drop the writer and remove the file.
            drop(writer);
            let _ = tokio::fs::remove_file(&path).await;
            return Ok(());
        }

        let path_for_finish = path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut writer = writer;
            writer.finish().map_err(Error::Arrow)?;
            let buf = writer.into_inner().map_err(Error::Arrow)?;
            let file = buf
                .into_inner()
                .map_err(|e| Error::Writer(format!("flush bufwriter: {e}")))?;
            file.sync_data()
                .map_err(|e| Error::Writer(format!("fsync {}: {e}", path_for_finish.display())))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Writer(format!("join error: {e}")))??;

        let path_for_read = path.clone();
        let batches = tokio::task::spawn_blocking(move || read_all_batches(&path_for_read))
            .await
            .map_err(|e| Error::Writer(format!("join error: {e}")))??;

        table.write_record_batches(batches).await?;

        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| Error::Writer(format!("remove {}: {e}", path.display())))?;

        Ok(())
    }

    /// Replay every leftover spool file in `spool_dir` belonging to
    /// `table`. Each file: read its batches, commit to Iceberg, then
    /// remove. Truncated trailing batches are dropped silently (kill -9
    /// mid-append). Schema mismatch surfaces from
    /// `IcebergTable::write_record_batches`.
    pub(super) async fn replay_dir<T>(spool_dir: &Path, table: &IcebergTable<T>) -> Result<()>
    where
        T: Serialize + Send + Sync + 'static,
    {
        if !spool_dir.exists() {
            return Ok(());
        }
        let prefix = table_prefix_for(table).await?;
        let paths = collect_spool_files(spool_dir, &prefix).await?;

        for path in paths {
            tracing::info!(path = %path.display(), "replaying spool file");
            let path_for_read = path.clone();
            let batches = tokio::task::spawn_blocking(move || read_all_batches(&path_for_read))
                .await
                .map_err(|e| Error::Writer(format!("join error: {e}")))??;

            if batches.is_empty() {
                tracing::info!(path = %path.display(), "spool file empty, removing");
            } else {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                tracing::info!(
                    path = %path.display(),
                    batches = batches.len(),
                    records = total,
                    "committing replayed batches",
                );
                table.write_record_batches(batches).await?;
            }

            tokio::fs::remove_file(&path)
                .await
                .map_err(|e| Error::Writer(format!("remove {}: {e}", path.display())))?;
        }
        Ok(())
    }
}

async fn arrow_schema_for<T>(table: &IcebergTable<T>) -> Result<SchemaRef> {
    let guard = table.table.read().await;
    let iceberg_schema = guard.metadata().current_schema();
    let arrow_schema = schema_to_arrow_schema(iceberg_schema).map_err(Error::Iceberg)?;
    Ok(Arc::new(arrow_schema))
}

async fn table_prefix_for<T>(table: &IcebergTable<T>) -> Result<String> {
    let guard = table.table.read().await;
    let identifier = guard.identifier();
    let namespace = identifier.namespace().to_url_string();
    let name = identifier.name();
    Ok(format!("{}__{}", sanitize(&namespace), sanitize(name)))
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

async fn open_new_writer(
    spool_dir: &Path,
    prefix: &str,
    schema: SchemaRef,
) -> Result<(PathBuf, StreamWriter<BufWriter<File>>)> {
    let filename = format!("{prefix}__{}.{SPOOL_FILE_EXT}", Uuid::now_v7());
    let path = spool_dir.join(filename);
    let path_for_create = path.clone();
    let writer = tokio::task::spawn_blocking(move || -> Result<_> {
        let file = File::create(&path_for_create)
            .map_err(|e| Error::Writer(format!("create {}: {e}", path_for_create.display())))?;
        let buf = BufWriter::new(file);
        StreamWriter::try_new(buf, &schema).map_err(Error::Arrow)
    })
    .await
    .map_err(|e| Error::Writer(format!("join error: {e}")))??;
    Ok((path, writer))
}

fn read_all_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file =
        File::open(path).map_err(|e| Error::Writer(format!("open {}: {e}", path.display())))?;
    let reader = match StreamReader::try_new(BufReader::new(file), None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "spool file has unreadable header, treating as empty",
            );
            return Ok(Vec::new());
        }
    };
    let mut batches = Vec::new();
    for batch in reader {
        match batch {
            Ok(b) => batches.push(b),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "stopping replay early at malformed batch (likely truncated tail)",
                );
                break;
            }
        }
    }
    Ok(batches)
}

async fn collect_spool_files(spool_dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut entries = tokio::fs::read_dir(spool_dir)
        .await
        .map_err(|e| Error::Writer(format!("read_dir {}: {e}", spool_dir.display())))?;
    let mut paths = Vec::new();
    let prefix_full = format!("{prefix}__");
    let suffix = format!(".{SPOOL_FILE_EXT}");
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| Error::Writer(format!("next_entry: {e}")))?
    {
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|f| f.to_str()) else {
            continue;
        };
        if filename.starts_with(&prefix_full) && filename.ends_with(&suffix) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}
