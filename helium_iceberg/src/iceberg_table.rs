use crate::catalog::Catalog;
use crate::writer::{BranchWriter, DataWriter};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_json::reader::ReaderBuilder;
use async_trait::async_trait;
use iceberg::arrow::{schema_to_arrow_schema, RecordBatchPartitionSplitter};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use serde::Serialize;
use std::sync::Arc;

pub struct IcebergTable {
    pub(crate) catalog: Catalog,
    pub(crate) table: Table,
}

pub struct IcebergTableBuilder {
    catalog: Catalog,
    namespace: String,
    table_name: String,
}

impl IcebergTableBuilder {
    pub fn new(
        catalog: Catalog,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog,
            namespace: namespace.into(),
            table_name: table_name.into(),
        }
    }

    pub async fn build(self) -> Result<IcebergTable> {
        IcebergTable::from_catalog(self.catalog, self.namespace, self.table_name).await
    }
}

impl IcebergTable {
    pub fn builder(
        catalog: Catalog,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> IcebergTableBuilder {
        IcebergTableBuilder::new(catalog, namespace, table_name)
    }

    /// Create an `IcebergTable` from an existing catalog connection.
    pub async fn from_catalog(
        catalog: Catalog,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let table = catalog.load_table(namespace, table_name).await?;
        Ok(Self { catalog, table })
    }

    fn records_to_batch<T: Serialize>(&self, records: &[T]) -> Result<RecordBatch> {
        let iceberg_schema = self.table.metadata().current_schema();
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

    /// Reload table metadata from the catalog.
    pub(crate) async fn reload(&mut self) -> Result {
        use iceberg::Catalog as IcebergCatalog;
        self.table = self
            .catalog
            .as_ref()
            .load_table(self.table.identifier())
            .await
            .map_err(Error::Iceberg)?;
        Ok(())
    }

    /// Create a [`StagedWriter`] session for the write-audit-publish workflow.
    ///
    /// This creates a branch and returns a session that supports multiple
    /// writes followed by `publish()`. The `wap_id` is used as both the
    /// branch name and the snapshot summary identifier.
    pub async fn staged_writer<T>(
        &mut self,
        wap_id: impl Into<String>,
    ) -> Result<crate::staged_writer::StagedWriter<'_, T, Self>>
    where
        T: Serialize + Send + Sync + 'static,
    {
        crate::staged_writer::StagedWriter::new(self, wap_id).await
    }

    /// Create an idempotent [`StagedWriter`] that detects prior WAP state on retry.
    ///
    /// Uses the `wap_id` to detect whether this WAP has already been partially
    /// or fully completed, and recovers appropriately:
    ///
    /// - **Not started**: creates a fresh `StagedWriter`
    /// - **Stale branch** (crash between create & commit): deletes branch, creates fresh
    /// - **Written but not published**: publishes the branch, returns `AlreadyComplete`
    /// - **Already published**: returns `AlreadyComplete` (no-op)
    pub async fn idempotent_staged_writer<T>(
        &mut self,
        wap_id: impl Into<String>,
    ) -> Result<crate::staged_writer::IdempotentWapOutcome<'_, T, Self>>
    where
        T: Serialize + Send + Sync + 'static,
    {
        use crate::staged_writer::IdempotentWapOutcome;

        let wap_id = wap_id.into();
        self.reload().await?;

        let wap_id_found = has_wap_id(&self.table, &wap_id);
        let branch_exists = self.table.metadata().snapshot_for_ref(&wap_id).is_some();

        match detect_wap_state(wap_id_found, branch_exists) {
            WapState::NotStarted => {
                tracing::debug!(wap_id, "WAP not started, creating fresh writer");
                let writer = crate::staged_writer::StagedWriter::new(self, wap_id).await?;
                Ok(IdempotentWapOutcome::Writer(writer))
            }
            WapState::StaleBranch => {
                tracing::debug!(wap_id, "stale branch detected, deleting and recreating");
                crate::branch::delete_branch(&self.catalog, &self.table, &wap_id).await?;
                self.reload().await?;
                let writer = crate::staged_writer::StagedWriter::new(self, wap_id).await?;
                Ok(IdempotentWapOutcome::Writer(writer))
            }
            WapState::WrittenNotPublished => {
                tracing::debug!(wap_id, "written but not published, publishing now");
                crate::branch::publish_branch(&self.catalog, &self.table, &wap_id).await?;
                Ok(IdempotentWapOutcome::AlreadyComplete)
            }
            WapState::AlreadyPublished => {
                tracing::debug!(wap_id, "already published, nothing to do");
                Ok(IdempotentWapOutcome::AlreadyComplete)
            }
        }
    }

    /// Write a record batch to data files without committing.
    pub(crate) async fn write_data_files(
        &self,
        batch: RecordBatch,
    ) -> Result<Vec<iceberg::spec::DataFile>> {
        let schema = self.table.metadata().current_schema().clone();
        let partition_spec = self.table.metadata().default_partition_spec().clone();
        let file_io = self.table.file_io().clone();

        let location_generator =
            DefaultLocationGenerator::new(self.table.metadata().clone()).map_err(Error::Iceberg)?;
        let timestamp_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .map_err(|e| Error::Writer(format!("failed to get system time: {e}")))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("{timestamp_millis}"),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(Default::default(), schema.clone());

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

    async fn write_and_commit(&self, batch: RecordBatch) -> Result {
        let data_files = self.write_data_files(batch).await?;
        self.commit_files(data_files).await
    }

    async fn commit_files(&self, data_files: Vec<iceberg::spec::DataFile>) -> Result {
        if data_files.is_empty() {
            return Ok(());
        }

        let tx = Transaction::new(&self.table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).map_err(Error::Iceberg)?;

        tx.commit(self.catalog.as_ref())
            .await
            .map_err(Error::Iceberg)?;

        tracing::debug!("committed data files to iceberg table");
        Ok(())
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static> DataWriter<T> for IcebergTable {
    async fn write(&self, records: Vec<T>) -> Result {
        if records.is_empty() {
            return Ok(());
        }

        let batch = self.records_to_batch(&records)?;
        self.write_and_commit(batch).await
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static> BranchWriter<T> for IcebergTable {
    /// Create a branch from the current main snapshot.
    async fn create_branch(&mut self, branch_name: &str) -> Result {
        self.reload().await?;
        crate::branch::create_branch(&self.catalog, &self.table, branch_name).await
    }

    /// Write records to a named branch (not main).
    async fn write_to_branch(
        &mut self,
        branch_name: &str,
        records: Vec<T>,
        wap_id: &str,
    ) -> Result {
        if records.is_empty() {
            return Ok(());
        }

        self.reload().await?;
        let batch = self.records_to_batch(&records)?;
        let data_files = self.write_data_files(batch).await?;
        crate::branch::commit_to_branch(&self.catalog, &self.table, branch_name, data_files, wap_id)
            .await
    }

    /// Fast-forward main to a branch's snapshot, then delete the branch.
    async fn publish_branch(&mut self, branch_name: &str) -> Result {
        self.reload().await?;
        crate::branch::publish_branch(&self.catalog, &self.table, branch_name).await
    }

    /// Delete a branch.
    async fn delete_branch(&mut self, branch_name: &str) -> Result {
        self.reload().await?;
        crate::branch::delete_branch(&self.catalog, &self.table, branch_name).await
    }
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
