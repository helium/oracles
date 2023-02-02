use crate::Result;
use serde::Deserialize;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres,
};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Max open connections to the database. If absent a default is calculated
    /// by application code
    pub max_connections: Option<u32>,
    /// URL to access the postgres database. For example:
    /// postgres://postgres:postgres@127.0.0.1:5432/mobile_index_db If the url
    /// is not specified the following environment variables are used to pick up
    /// the database settings:
    ///
    ///  * `PGHOST`
    ///  * `PGPORT`
    ///  * `PGUSER`
    ///  * `PGPASSWORD`
    ///  * `PGDATABASE`
    ///  * `PGSSLROOTCERT`
    ///  * `PGSSLMODE`
    ///  * `PGAPPNAME`
    pub url: Option<String>,
}

impl Settings {
    pub async fn connect(&self, default_max_connections: usize) -> Result<Pool<Postgres>> {
        let connect_options = if let Some(url) = &self.url {
            url.parse()?
        } else {
            PgConnectOptions::new()
        };
        let pool = PgPoolOptions::new()
            .max_connections(
                self.max_connections
                    .unwrap_or(default_max_connections as u32),
            )
            .connect_with(connect_options)
            .await?;
        Ok(pool)
    }
}
