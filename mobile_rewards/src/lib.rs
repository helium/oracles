mod cell_type;
pub mod decimal_scalar;
mod error;
pub mod pending_txn;
pub mod server;
mod settings;
pub mod token_type;
pub mod traits;
pub mod txn_status;
mod uuid;

pub use cell_type::CellType;
pub use decimal_scalar::Mobile;
pub use error::{Error, Result};
pub use server::Server;
pub use settings::Settings;
pub use uuid::Uuid;

use std::{env, path::Path};

pub fn write_json<T: ?Sized + serde::Serialize>(
    fname_prefix: &str,
    after_ts: u64,
    before_ts: u64,
    data: &T,
) -> Result {
    let tmp_output_dir = env::var("TMP_OUTPUT_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let fname = format!("{}-{}-{}.json", fname_prefix, after_ts, before_ts);
    let fpath = Path::new(&tmp_output_dir).join(fname);
    std::fs::write(Path::new(&fpath), serde_json::to_string_pretty(data)?)?;
    Ok(())
}
