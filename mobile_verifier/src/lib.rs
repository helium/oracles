mod cell_type;
mod error;
mod mobile;
mod reward_share;
mod reward_speed_share;

pub mod cli;
pub mod server;
pub mod subnetwork_rewards;

pub use error::{Error, Result};
pub use server::run_server;

use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::io;

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
