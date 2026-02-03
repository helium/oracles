use crate::{Error, Result, Settings};
use arrow_array::RecordBatch;
use arrow_json::reader::ReaderBuilder;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
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
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send + Sync + 'static,
{
    async fn write(&self, records: Vec<T>) -> Result;

    async fn write_stream<S>(&self, stream: S) -> Result
    where
        S: Stream<Item = T> + Send + 'static;
}

pub struct IcebergTable {
    catalog: Arc<RestCatalog>,
    table: Table,
}

pub struct IcebergTableBuilder {
    settings: Settings,
    namespace: String,
    table_name: String,
}

impl IcebergTableBuilder {
    pub fn new(
        settings: Settings,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            settings,
            namespace: namespace.into(),
            table_name: table_name.into(),
        }
    }

    pub async fn build(self) -> Result<IcebergTable> {
        IcebergTable::connect(self.settings, self.namespace, self.table_name).await
    }
}

impl IcebergTable {
    pub fn builder(
        settings: Settings,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> IcebergTableBuilder {
        IcebergTableBuilder::new(settings, namespace, table_name)
    }

    pub async fn connect(
        settings: Settings,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let namespace = namespace.into();
        let table_name = table_name.into();

        let mut config = HashMap::new();
        config.insert(
            REST_CATALOG_PROP_URI.to_string(),
            settings.catalog_uri.clone(),
        );
        if let Some(ref warehouse) = settings.warehouse {
            config.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }

        let catalog = RestCatalogBuilder::default()
            .load(&settings.catalog_name, config)
            .await
            .map_err(Error::Iceberg)?;

        let namespace_ident = NamespaceIdent::new(namespace.clone());
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| match e.kind() {
                iceberg::ErrorKind::DataInvalid => Error::TableNotFound {
                    namespace,
                    table: table_name,
                },
                _ => Error::Iceberg(e),
            })?;

        Ok(Self {
            catalog: Arc::new(catalog),
            table,
        })
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

    async fn write_and_commit(&self, batch: RecordBatch) -> Result {
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

        let data_files = fanout_writer.close().await.map_err(Error::Iceberg)?;

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

    async fn write_stream<S>(&self, stream: S) -> Result
    where
        S: Stream<Item = T> + Send + 'static,
    {
        let records: Vec<T> = stream.collect().await;
        self.write(records).await
    }
}
