pub mod error;
pub mod gateway;
pub mod loader;
pub mod meta;
pub mod server;

pub use error::{Error, Result};
pub use poc_store::datetime_from_epoch;

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}
