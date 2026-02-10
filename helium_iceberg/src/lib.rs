extern crate tls_init;

mod branch;
mod catalog;
mod error;
mod iceberg_table;
mod settings;
mod table_creator;
mod writer;

#[cfg(feature = "test-harness")]
pub mod test_harness;

pub use catalog::Catalog;
pub use error::{Error, Result};
pub use iceberg_table::IcebergTable;
pub use settings::{AuthConfig, S3Config, Settings};
pub use table_creator::{
    FieldDefinition, PartitionDefinition, SortFieldDefinition, TableCreator, TableDefinition,
    TableDefinitionBuilder,
};
pub use writer::{DataWriter, StagedWriter, WriteOutcome};

// Re-export iceberg types for ergonomic API usage
pub use iceberg::spec::{NullOrder, PrimitiveType, SortDirection, Transform, Type};

#[cfg(feature = "test-harness")]
pub use test_harness::{HarnessConfig, IcebergTestHarness};

pub type BoxedDataWriter<T> = std::sync::Arc<dyn DataWriter<T>>;

#[cfg(test)]
tls_init::include_tls_tests!();
