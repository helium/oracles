mod cell_type;
mod error;
mod mobile;
mod reward_share;
mod reward_speed_share;

pub mod cli;
pub mod server;
pub mod subnetwork_rewards;

pub use error::{Error, Result};
pub use server::Server;

use poc_store::FileStore;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::io;

pub async fn write_json<T: ?Sized + serde::Serialize>(
    file_store: &FileStore,
    fname_prefix: &str,
    after_ts: u64,
    before_ts: u64,
    data: &T,
) -> Result {
    let fname = format!("{}-{}-{}.json", fname_prefix, after_ts, before_ts);
    file_store
        .put_bytes(&fname, serde_json::to_string_pretty(data)?.into_bytes())
        .await?;
    Ok(())
}

pub fn env_var<T>(key: &str, default: T) -> Result<T>
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

fn bones_to_u64(decimal: Decimal) -> u64 {
    // One bone is one million mobiles
    (decimal * dec!(1_000_000)).to_u64().unwrap()
}

fn cell_share_to_u64(decimal: Decimal) -> u64 {
    (decimal * dec!(10)).to_u64().unwrap()
}
