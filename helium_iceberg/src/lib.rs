extern crate tls_init;

mod error;
mod memory_writer;
mod settings;
mod writer;

pub use error::{Error, Result};
pub use memory_writer::MemoryDataWriter;
pub use settings::Settings;
pub use writer::{DataWriter, IcebergTable, IcebergTableBuilder};

#[cfg(test)]
tls_init::include_tls_tests!();
