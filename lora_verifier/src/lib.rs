pub mod entropy;
pub mod error;
pub mod follower;
pub mod last_beacon;
pub mod loader;
pub mod meta;
pub mod poc;
pub mod poc_report;
pub mod runner;
pub mod traits;

pub use error::{Error, Result};
pub use file_store::datetime_from_epoch;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::io;

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
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
