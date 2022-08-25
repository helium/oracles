mod cell_type;
pub mod cli;
pub mod decimal_scalar;
mod error;
pub mod follower;
pub mod keypair;
pub mod meta;
pub mod pending_txn;
mod public_key;
pub mod reward_share;
pub mod server;
pub mod subnetwork_reward;
pub mod subnetwork_rewards;
pub mod token_type;
pub mod traits;
pub mod transaction;
pub mod txn_status;
mod uuid;

pub use cell_type::CellType;
use chrono::{DateTime, NaiveDateTime, Utc};
pub use decimal_scalar::Mobile;
pub use error::{Error, Result};
pub use keypair::Keypair;
pub use public_key::PublicKey;
pub use uuid::Uuid;

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::{io, path::Path};

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

pub fn write_json<T: ?Sized + serde::Serialize>(
    fname_prefix: &str,
    after_ts: u64,
    before_ts: u64,
    data: &T,
) -> Result {
    let tmp_output_dir = env_var("TMP_OUTPUT_DIR", "/tmp".to_string())?;
    let fname = format!("{}-{}-{}.json", fname_prefix, after_ts, before_ts);
    let fpath = Path::new(&tmp_output_dir).join(&fname);
    std::fs::write(Path::new(&fpath), serde_json::to_string_pretty(data)?)?;
    Ok(())
}

fn env_var<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    match dotenv::var(key) {
        Ok(v) => v
            .parse::<T>()
            .map_err(|_err| Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        Err(dotenv::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(default),
        Err(err) => Err(Error::from(err)),
    }
}

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}
