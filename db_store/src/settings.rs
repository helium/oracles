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
    /// Min open connection tot he database, if absent the default from sqlx is used
    pub min_connections: Option<u32>,
    /// Maximum idle duration for idle connections in seconds, if absent the default from sqlx is
    /// used.  0 mean infinite timeout
    pub idle_timeout: Option<u64>,
    /// Maximum lifetime for connections in seconds, if absent the default from sqlx is
    /// used.  0 mean infinite timeout
    pub max_lifetime: Option<u64>,
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

        let pool_options = self.pool_options(default_max_connections);
        Ok(pool_options.connect_with(connect_options).await?)
    }

    fn pool_options(&self, default_max_connections: usize) -> PgPoolOptions {
        let mut pool_options = PgPoolOptions::new().max_connections(
            self.max_connections
                .unwrap_or(default_max_connections as u32),
        );

        pool_options = if self.min_connections.is_some() {
            pool_options.min_connections(self.min_connections.unwrap())
        } else {
            pool_options
        };

        pool_options = if self.idle_timeout.is_some() {
            let idle_timeout = self.idle_timeout.unwrap();
            if idle_timeout == 0 {
                pool_options.idle_timeout(None)
            } else {
                let duration = std::time::Duration::from_secs(idle_timeout);
                pool_options.idle_timeout(duration)
            }
        } else {
            pool_options
        };

        if self.max_lifetime.is_some() {
            let max_lifetime = self.max_lifetime.unwrap();
            if max_lifetime == 0 {
                pool_options.max_lifetime(None)
            } else {
                let duration = std::time::Duration::from_secs(max_lifetime);
                pool_options.max_lifetime(duration)
            }
        } else {
            pool_options
        }
    }
}
