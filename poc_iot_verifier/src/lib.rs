pub mod entropy;
pub mod error;
pub mod last_beacon;
pub mod loader;
pub mod meta;
pub mod poc;
pub mod poc_report;
pub mod purger;
pub mod runner;
pub mod traits;

pub use error::{Error, Result};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub async fn mk_db_pool(size: u32) -> Result<Pool<Postgres>> {
    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(size)
        .connect(&db_connection_str)
        .await?;
    Ok(pool)
}
