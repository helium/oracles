pub mod cli;
pub mod receipt_txn;
pub mod server;
mod settings;

pub use settings::Settings;

// Number of files to load from S3
pub const LOADER_WORKERS: usize = 2;
// Number of streams to process
pub const STORE_WORKERS: usize = 5;
