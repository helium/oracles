use crate::catalog::Catalog;
use crate::writer::{BranchPublisher, BranchWriter, DataWriter, WriteSession};
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

    /// Returns the `additional_properties` from the main branch's latest snapshot summary.
    /// Returns an empty map if no snapshot exists yet (fresh table).
    pub async fn snapshot_properties(&self) -> Result<HashMap<String, String>> {
        reload_table(&self.catalog, &self.table).await?;
        let table = self.table.read().await;
        let properties = table
            .metadata()
            .snapshot_for_ref(iceberg::spec::MAIN_BRANCH)
            .map(|snapshot| snapshot.summary().additional_properties.clone())
            .unwrap_or_default();
        Ok(properties)
    }

    async fn write_and_commit(&self, batch: RecordBatch) -> Result {
        let data_files = write_data_files(&self.table, batch).await?;
        self.commit_files(data_files).await
    }

    async fn commit_files(&self, data_files: Vec<iceberg::spec::DataFile>) -> Result {
        if data_files.is_empty() {
            return Ok(());
        }

        let table = self.table.read().await;
        let tx = IcebergTransaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).map_err(Error::Iceberg)?;
        drop(table);

        tx.commit(self.catalog.as_ref())
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
        self.write_and_commit(batch).await
    }

    async fn begin(&self, wap_id: &str) -> Result<WriteSession<T>> {
        reload_table(&self.catalog, &self.table).await?;

        let table = self.table.read().await;
        let wap_id_found = has_wap_id(&table, wap_id);
        let branch_exists = table.metadata().snapshot_for_ref(wap_id).is_some();
        drop(table);

        match detect_wap_state(wap_id_found, branch_exists) {
            WapState::NotStarted => {
                tracing::debug!(wap_id, "WAP not started, creating fresh branch");
                let table_guard = self.table.read().await;
                crate::branch::create_branch(&self.catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                Ok(WriteSession::Writer(Box::new(IcebergBranchWriter {
                    catalog: self.catalog.clone(),
                    table: Arc::clone(&self.table),
                    branch_name: wap_id.to_string(),
                    _phantom: PhantomData,
                })))
            }
            WapState::StaleBranch => {
                tracing::debug!(wap_id, "stale branch detected, deleting and recreating");
                let table_guard = self.table.read().await;
                crate::branch::delete_branch(&self.catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                reload_table(&self.catalog, &self.table).await?;
                let table_guard = self.table.read().await;
                crate::branch::create_branch(&self.catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                Ok(WriteSession::Writer(Box::new(IcebergBranchWriter {
                    catalog: self.catalog.clone(),
                    table: Arc::clone(&self.table),
                    branch_name: wap_id.to_string(),
                    _phantom: PhantomData,
                })))
            }
            WapState::WrittenNotPublished => {
                tracing::debug!(wap_id, "written but not published");
                Ok(WriteSession::Publisher(Box::new(IcebergBranchPublisher {
                    catalog: self.catalog.clone(),
                    table: Arc::clone(&self.table),
                    branch_name: wap_id.to_string(),
                })))
            }
            WapState::AlreadyPublished => {
                tracing::debug!(wap_id, "already published, nothing to do");
                Ok(WriteSession::Complete)
            }
        }
    }
}

struct IcebergBranchWriter<T> {
    catalog: Catalog,
    table: Arc<RwLock<Table>>,
    branch_name: String,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T: Serialize + Send + 'static> BranchWriter<T> for IcebergBranchWriter<T> {
    async fn write(
        self: Box<Self>,
        records: Vec<T>,
        custom_properties: HashMap<String, String>,
    ) -> Result<Box<dyn BranchPublisher>> {
        if !records.is_empty() {
            reload_table(&self.catalog, &self.table).await?;
            let table_guard = self.table.read().await;
            let batch = records_to_batch(&table_guard, &records)?;
            drop(table_guard);
            let data_files = write_data_files(&self.table, batch).await?;
            let table_guard = self.table.read().await;
            crate::branch::commit_to_branch(
                &self.catalog,
                &table_guard,
                &self.branch_name,
                data_files,
                &self.branch_name,
                custom_properties,
            )
            .await?;
        }
        Ok(Box::new(IcebergBranchPublisher {
            catalog: self.catalog,
            table: self.table,
            branch_name: self.branch_name,
        }))
    }
}

struct IcebergBranchPublisher {
    catalog: Catalog,
    table: Arc<RwLock<Table>>,
    branch_name: String,
}

#[async_trait]
impl BranchPublisher for IcebergBranchPublisher {
    async fn publish(self: Box<Self>) -> Result {
        reload_table(&self.catalog, &self.table).await?;
        let table_guard = self.table.read().await;
        crate::branch::publish_branch(&self.catalog, &table_guard, &self.branch_name).await
    }
}

async fn reload_table(catalog: &Catalog, table: &RwLock<Table>) -> Result {
    use iceberg::Catalog as IcebergCatalog;
    let identifier = table.read().await.identifier().clone();
    let new_table = catalog
        .as_ref()
        .load_table(&identifier)
        .await
        .map_err(Error::Iceberg)?;
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
    let timestamp_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .map_err(|e| Error::Writer(format!("failed to get system time: {e}")))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{timestamp_millis}"),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WapState {
    NotStarted,
    StaleBranch,
    WrittenNotPublished,
    AlreadyPublished,
}

fn detect_wap_state(wap_id_found: bool, branch_exists: bool) -> WapState {
    match (wap_id_found, branch_exists) {
        (false, false) => WapState::NotStarted,
        (false, true) => WapState::StaleBranch,
        (true, true) => WapState::WrittenNotPublished,
        (true, false) => WapState::AlreadyPublished,
    }
}

fn has_wap_id(table: &Table, wap_id: &str) -> bool {
    table.metadata().snapshots().any(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(crate::branch::WAP_ID_KEY)
            .is_some_and(|v| v == wap_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_wap_state_not_started() {
        assert_eq!(detect_wap_state(false, false), WapState::NotStarted,);
    }

    #[test]
    fn test_detect_wap_state_stale_branch() {
        assert_eq!(detect_wap_state(false, true), WapState::StaleBranch,);
    }

    #[test]
    fn test_detect_wap_state_written_not_published() {
        assert_eq!(detect_wap_state(true, true), WapState::WrittenNotPublished,);
    }

    #[test]
    fn test_detect_wap_state_already_published() {
        assert_eq!(detect_wap_state(true, false), WapState::AlreadyPublished,);
    }
}
