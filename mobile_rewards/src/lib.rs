mod cell_type;
pub mod pending_txn;
pub mod server;
mod server_metrics;
mod settings;
pub mod token_type;
pub mod txn_status;
mod uuid;

pub use cell_type::CellType;
pub use server::Server;
pub use settings::Settings;
pub use uuid::Uuid;

use std::{env, path::Path};

pub fn write_json<T: ?Sized + serde::Serialize>(
    fname_prefix: &str,
    after_ts: u64,
    before_ts: u64,
    data: &T,
) -> anyhow::Result<()> {
    let tmp_output_dir = env::var("TMP_OUTPUT_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let fname = format!("{fname_prefix}-{after_ts}-{before_ts}.json");
    let fpath = Path::new(&tmp_output_dir).join(fname);
    std::fs::write(Path::new(&fpath), serde_json::to_string_pretty(data)?)?;
    Ok(())
}
