use crate::Result;
use http::Uri;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Max open connections to the database. If absent a default is calculated
    /// by application code
    pub max_connections: Option<u32>,
    /// URL to access the postgres database. For example:
    /// postgres://postgres:postgres@127.0.0.1:5432/mobile_index_db
    #[serde(with = "http_serde::uri")]
    pub url: Uri,
}

impl Settings {
    pub async fn connect(&self, default_max_connections: usize) -> Result<Pool<Postgres>> {
        let pool = PgPoolOptions::new()
            .max_connections(
                self.max_connections
                    .unwrap_or(default_max_connections as u32),
            )
            .connect(&self.url.to_string())
            .await?;
        Ok(pool)
    }
}
