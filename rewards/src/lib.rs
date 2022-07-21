mod cell_type;
pub mod decimal_scalar;
mod error;
pub mod follower;
pub mod gateway;
mod public_key;
pub mod server;
mod uuid;

pub use cell_type::CellType;
use chrono::{DateTime, NaiveDateTime, Utc};
pub use decimal_scalar::Mobile;
pub use error::{Error, Result};
pub use public_key::PublicKey;
pub use uuid::Uuid;

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::io;

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub block_height: u64,
}

impl Trigger {
    pub fn new(block_height: u64) -> Self {
        Self { block_height }
    }
}
