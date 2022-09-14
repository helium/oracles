pub mod cli;
mod error;
pub mod keypair;
pub mod receipt_txn;
pub mod server;
pub mod txn_service;

use chrono::{DateTime, NaiveDateTime, Utc};
pub use error::{Error, Result};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

pub fn datetime_from_naive(v: NaiveDateTime) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(v, Utc)
}
