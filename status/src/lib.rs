pub mod error;
pub mod gateway;
pub mod loader;
pub mod meta;
pub mod server;

mod public_key;

pub use error::{Error, Result};
pub use poc_store::datetime_from_epoch;
pub use poc_store::env_var;
pub use public_key::PublicKey;

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}
