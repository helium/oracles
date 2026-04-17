//! Iceberg table wrapper with idempotent writes.
//!
//! Idempotency is keyed by an application-provided `id` that gets stamped
//! onto the snapshot summary under the `helium.write_id` property. Before a
//! write, if any snapshot on main already carries that id, the write is a
//! no-op.
//!
//! Caveats:
//! 1. Snapshot expiration invalidates idempotency. If `expire_snapshots`
//!    removes the snapshot carrying a given `helium.write_id`, a replay
//!    re-commits. For retention longer than your snapshot history, track
//!    processed ids externally.
//! 2. Cross-table atomicity is not provided — partial failure across
//!    multiple writes leaves some tables committed and others not; retry
//!    completes the rest.
//! 3. Orphaned parquet files if concurrent `write_idempotent` calls race
//!    on the same id. Iceberg's orphan-file cleanup handles these.
//! 4. The legacy `wap.id` property is still read during the idempotency
//!    check for a migration window. Safe to remove once all in-flight
//!    snapshots carrying the old property have expired.

use crate::catalog::Catalog;
use crate::writer::DataWriter;
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_json::reader::ReaderBuilder;
use async_trait::async_trait;
use iceberg::arrow::{schema_to_arrow_schema, RecordBatchPartitionSplitter};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction as IcebergTransaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Snapshot summary property used to key idempotent writes.
pub(crate) const WRITE_ID_PROPERTY: &str = "helium.write_id";

/// Legacy property name from the previous branch-based WAP workflow. Read
/// during the idempotency check so in-flight files written before this
/// module was refactored aren't reprocessed. Safe to remove once all
/// snapshots carrying this property have expired.
const LEGACY_WAP_ID_PROPERTY: &str = "wap.id";

pub struct IcebergTable<T> {
    pub(crate) catalog: Catalog,
    pub(crate) table: Arc<RwLock<Table>>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T> IcebergTable<T> {
    /// Create an `IcebergTable` from an existing catalog connection.
    pub async fn from_catalog(
        catalog: Catalog,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let table = catalog.load_table(namespace, table_name).await?;
        Ok(Self {
            catalog,
            table: Arc::new(RwLock::new(table)),
            _phantom: PhantomData,
        })
    }

    async fn write_and_commit(
        &self,
        batch: RecordBatch,
        snapshot_properties: HashMap<String, String>,
    ) -> Result {
        let data_files = write_data_files(&self.table, batch).await?;
        self.commit_files(data_files, snapshot_properties).await
    }

    async fn commit_files(
        &self,
        data_files: Vec<iceberg::spec::DataFile>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result {
        if data_files.is_empty() {
            return Ok(());
        }

        self.catalog
            .with_auth(|| {
                let data_files = data_files.clone();
                let snapshot_properties = snapshot_properties.clone();
                async {
                    let table = self.table.read().await;
                    let tx = IcebergTransaction::new(&table);
                    let action = tx
                        .fast_append()
                        .set_snapshot_properties(snapshot_properties)
                        .add_data_files(data_files);
                    let tx = action.apply(tx)?;
                    drop(table);
                    tx.commit(self.catalog.as_ref()).await
                }
            })
            .await
            .map_err(Error::Iceberg)?;

        tracing::debug!("committed data files to iceberg table");
        Ok(())
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static> DataWriter<T> for IcebergTable<T> {
    async fn write(&self, records: Vec<T>) -> Result {
        if records.is_empty() {
            return Ok(());
        }

        let table = self.table.read().await;
        let batch = records_to_batch(&table, &records)?;
        drop(table);
        self.write_and_commit(batch, HashMap::new()).await
    }

    async fn write_idempotent(&self, id: &str, records: Vec<T>) -> Result {
        reload_table(&self.catalog, &self.table).await?;

        if has_write_id(&*self.table.read().await, id) {
            tracing::debug!(id, "write_id already present, skipping");
            return Ok(());
        }

        if records.is_empty() {
            return Ok(());
        }

        let table = self.table.read().await;
        let batch = records_to_batch(&table, &records)?;
        drop(table);

        let props = HashMap::from([(WRITE_ID_PROPERTY.to_string(), id.to_string())]);
        self.write_and_commit(batch, props).await
    }
}

pub(crate) async fn reload_table(catalog: &Catalog, table: &RwLock<Table>) -> Result {
    let identifier = table.read().await.identifier().clone();
    let namespace = identifier.namespace().to_url_string();
    let table_name = identifier.name().to_string();

    let new_table = catalog.load_table(namespace, table_name).await?;
    *table.write().await = new_table;
    Ok(())
}

fn records_to_batch<T: Serialize>(table: &Table, records: &[T]) -> Result<RecordBatch> {
    let iceberg_schema = table.metadata().current_schema();
    let arrow_schema = schema_to_arrow_schema(iceberg_schema).map_err(Error::Iceberg)?;

    let mut decoder = ReaderBuilder::new(Arc::new(arrow_schema))
        .build_decoder()
        .map_err(|e| Error::Writer(format!("decoder error: {e}")))?;

    decoder
        .serialize(records)
        .map_err(|e| Error::Writer(format!("serialize error: {e}")))?;

    decoder
        .flush()
        .map_err(|e| Error::Writer(format!("flush error: {e}")))?
        .ok_or_else(|| Error::Writer("no batch produced".into()))
}

async fn write_data_files(
    table: &RwLock<Table>,
    batch: RecordBatch,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let table_guard = table.read().await;
    let schema = table_guard.metadata().current_schema().clone();
    let partition_spec = table_guard.metadata().default_partition_spec().clone();
    let file_io = table_guard.file_io().clone();
    let writer_props = build_writer_properties(table_guard.metadata().properties());

    let location_generator =
        DefaultLocationGenerator::new(table_guard.metadata().clone()).map_err(Error::Iceberg)?;
    drop(table_guard);
    // Use a v7 UUID (time-ordered + random) so concurrent writers never
    // collide on file paths, even with sub-millisecond-close timestamps.
    // A plain wall-clock prefix would let two writers generate the same
    // path, trip `fast_append`'s duplicate-file check, and fail the second
    // commit.
    let file_name_generator = DefaultFileNameGenerator::new(
        uuid::Uuid::now_v7().to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, schema.clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        file_io,
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let splitter =
        RecordBatchPartitionSplitter::try_new_with_computed_values(schema, partition_spec)
            .map_err(Error::Iceberg)?;

    let partitioned_batches = splitter.split(&batch).map_err(Error::Iceberg)?;

    let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);

    for (partition_key, partition_batch) in partitioned_batches {
        fanout_writer
            .write(partition_key, partition_batch)
            .await
            .map_err(Error::Iceberg)?;
    }

    fanout_writer.close().await.map_err(Error::Iceberg)
}

fn build_writer_properties(
    properties: &std::collections::HashMap<String, String>,
) -> parquet::file::properties::WriterProperties {
    let compression = match properties
        .get(crate::table_creator::PARQUET_COMPRESSION_CODEC)
        .map(|s| s.to_lowercase())
        .as_deref()
    {
        Some("zstd") => parquet::basic::Compression::ZSTD(Default::default()),
        _ => parquet::basic::Compression::UNCOMPRESSED,
    };

    parquet::file::properties::WriterProperties::builder()
        .set_compression(compression)
        .build()
}

fn has_write_id(table: &Table, id: &str) -> bool {
    table.metadata().snapshots().any(|snapshot| {
        let props = &snapshot.summary().additional_properties;
        props.get(WRITE_ID_PROPERTY).is_some_and(|v| v == id)
            || props.get(LEGACY_WAP_ID_PROPERTY).is_some_and(|v| v == id)
    })
}
