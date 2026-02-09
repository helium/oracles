extern crate tls_init;

mod branch;
mod catalog;
mod error;
mod iceberg_table;
mod memory_writer;
mod settings;
mod staged_writer;
mod table_creator;
mod writer;

pub use catalog::Catalog;
pub use error::{Error, Result};
pub use iceberg_table::IcebergTable;
pub use memory_writer::{MemoryBranchWriter, MemoryDataWriter};
pub use settings::{AuthConfig, Settings};
pub use staged_writer::{IdempotentWapOutcome, StagedPublisher, StagedWriter};
pub use table_creator::{
    FieldDefinition, PartitionDefinition, SortFieldDefinition, TableCreator, TableDefinition,
    TableDefinitionBuilder,
};
pub use writer::{BranchWriter, DataWriter};

// Re-export iceberg types for ergonomic API usage
pub use iceberg::spec::{NullOrder, PrimitiveType, SortDirection, Transform, Type};

#[cfg(test)]
tls_init::include_tls_tests!();
