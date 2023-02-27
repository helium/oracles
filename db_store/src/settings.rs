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
    pub max_connections: u32,

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
        shutdown: triggered::Listener,
    ) -> Result<(Pool<Postgres>, futures::future::BoxFuture<'static, Result>)> {
        match self.auth_type {
            AuthType::Postgres => match self.simple_connect().await {
                Ok(pool) => Ok((pool, Box::pin(async move { Ok(()) }))),
                Err(err) => Err(err),
            },
            AuthType::Iam => iam_auth_pool::connect(self, shutdown).await,
        }
    }

    async fn simple_connect(&self) -> Result<Pool<Postgres>> {
        let connect_options = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .username(&self.username)
            .password(self.postgres_password.as_ref().ok_or_else(|| {
                Error::InvalidConfiguration("postgres_password is required".to_string())
            })?);

        let pool = self.pool_options().connect_with(connect_options).await?;
        Ok(pool)
    }

    pub fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new().max_connections(self.max_connections)
    }
}
