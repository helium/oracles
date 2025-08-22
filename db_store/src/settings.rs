use std::path::PathBuf;

use crate::{iam_auth_pool, metric_tracker, Error, Result};
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    Pool, Postgres,
};

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Postgres,
    Iam,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// URL to access the postgres database, only used when
    /// the auth_type is Postgres
    #[serde(skip_serializing)]
    pub url: Option<String>,

    /// Optionally provided certificate authority
    pub ca_path: Option<PathBuf>,

    #[serde(default = "default_auth_type")]
    auth_type: AuthType,

    /// Db connection information only used when auth_type is Iam
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,

    pub iam_role_arn: Option<String>,
    pub iam_role_session_name: Option<String>,
    pub iam_duration_seconds: Option<i32>,
    pub iam_region: Option<String>,
}

fn default_auth_type() -> AuthType {
    AuthType::Postgres
}

fn default_max_connections() -> u32 {
    5
}

impl Settings {
    pub async fn connect(&self, app_name: &str) -> Result<Pool<Postgres>> {
        match self.auth_type {
            AuthType::Postgres => match self.simple_connect().await {
                Ok(pool) => {
                    metric_tracker::start(app_name, pool.clone()).await;
                    Ok(pool)
                }
                Err(err) => Err(err),
            },
            AuthType::Iam => {
                let pool = iam_auth_pool::connect(self).await?;
                metric_tracker::start(app_name, pool.clone()).await;
                Ok(pool)
            }
        }
    }

    async fn simple_connect(&self) -> Result<Pool<Postgres>> {
        let connect_options: PgConnectOptions = self
            .url
            .as_ref()
            .ok_or_else(|| Error::InvalidConfiguration("url is required".to_string()))?
            .parse()?;

        let connect_options = if let Some(ref ca_path) = self.ca_path {
            connect_options
                .ssl_mode(PgSslMode::VerifyCa)
                .ssl_root_cert(ca_path)
        } else {
            connect_options
        };

        let pool = self.pool_options().connect_with(connect_options).await?;
        Ok(pool)
    }

    pub fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new().max_connections(self.max_connections)
    }
}
