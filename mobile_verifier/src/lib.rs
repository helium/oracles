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

use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
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

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}
