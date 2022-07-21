pub mod file_sink;
pub mod file_source;

mod error;

pub use error::{Error, Result};
pub use file_sink::{FileSink, FileSinkBuilder};
pub use file_source::FileSource;
