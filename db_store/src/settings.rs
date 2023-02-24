use crate::{iam_auth_pool, Error, Result};
use serde::Deserialize;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres,
};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Postgres,
    Iam,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Max open connections to the database. If absent a default is calculated
    /// by application code
    pub max_connections: Option<u32>,

    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,

    #[serde(default = "default_auth_type")]
    auth_type: AuthType,

    postgres_password: Option<String>,

    pub iam_role_arn: Option<String>,
    pub iam_role_session_name: Option<String>,
    pub iam_duration_seconds: Option<i32>,
    pub iam_region: Option<String>,
}

fn default_auth_type() -> AuthType {
    AuthType::Postgres
}

impl Settings {
    pub async fn connect(
        &self,
        default_max_connections: usize,
        shutdown: triggered::Listener,
    ) -> Result<(Pool<Postgres>, futures::future::BoxFuture<'static, Result>)> {
        match self.auth_type {
            AuthType::Postgres => match self.simple_connect(default_max_connections).await {
                Ok(pool) => Ok((pool, Box::pin(async move { Ok(()) }))),
                Err(err) => Err(err),
            },
            AuthType::Iam => iam_auth_pool::connect(self, default_max_connections, shutdown).await,
        }
    }

    pub async fn simple_connect(&self, default_max_connections: usize) -> Result<Pool<Postgres>> {
        let connect_options = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .username(&self.username)
            .password(self.postgres_password.as_ref().ok_or_else(|| {
                Error::InvalidConfiguration("postgres_password is required".to_string())
            })?);

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
