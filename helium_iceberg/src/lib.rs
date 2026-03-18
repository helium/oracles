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
    FieldDefinition, ParquetCompression, PartitionDefinition, SortFieldDefinition, TableCreator,
    TableDefinition, TableDefinitionBuilder, PARQUET_COMPRESSION_CODEC,
};
pub use writer::{
    BoxedDataWriter, BranchPublisher, BranchTransaction, BranchWriter, DataWriter,
    IntoBoxedDataWriter,
};

// Re-export iceberg types for ergonomic API usage
pub use iceberg::spec::{NullOrder, PrimitiveType, SortDirection, Transform, Type};
pub use iceberg::{NamespaceIdent, TableIdent};

/// Converts a list of key-value pairs into a `HashMap`, only inserting the key
/// when the value is `Some`.
#[macro_export]
macro_rules! option_hashmap {
    ( $($key:expr => $maybe_val:expr),+ $(,)? ) => {
        {
            let mut map = std::collections::HashMap::new();
            $(
                if let Some(ref val) = $maybe_val {
                    map.insert($key.to_string(), val.to_string());
                }
            )+
            map
        }
    };
}

#[cfg(feature = "test-harness")]
pub use test_harness::{HarnessConfig, IcebergTestHarness};

#[cfg(test)]
tls_init::include_tls_tests!();
