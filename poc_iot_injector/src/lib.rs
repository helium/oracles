pub mod cli;
mod error;
pub mod keypair;
pub mod receipt_txn;
pub mod server;
pub mod txn_service;

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
