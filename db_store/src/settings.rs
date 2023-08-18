use crate::{iam_auth_pool, metric_tracker, Error, Result};
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Postgres,
    Iam,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub max_connections: u32,

    /// URL to access the postgres database, only used when
    /// the auth_type is Postgres
    pub url: Option<String>,

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

impl Settings {
    pub async fn connect(
        &self,
        app_name: &str,
        shutdown: triggered::Listener,
    ) -> Result<(Pool<Postgres>, futures::future::BoxFuture<'static, Result>)> {
        match self.auth_type {
            AuthType::Postgres => match self.simple_connect().await {
                Ok(pool) => Ok((
                    pool.clone(),
                    metric_tracker::start(app_name, pool, shutdown).await?,
                )),
                Err(err) => Err(err),
            },
            AuthType::Iam => {
                let (pool, iam_auth_handle) =
                    iam_auth_pool::connect(self, shutdown.clone()).await?;
                let metric_handle = metric_tracker::start(app_name, pool.clone(), shutdown).await?;

                let handle =
                    tokio::spawn(async move { tokio::try_join!(iam_auth_handle, metric_handle) });

                Ok((
                    pool,
                    Box::pin(async move {
                        match handle.await {
                            Ok(Err(err)) => Err(err),
                            Err(err) => Err(Error::from(err)),
                            Ok(_) => Ok(()),
                        }
                    }),
                ))
            }
        }
    }

    pub async fn connect_tm(&self, app_name: &str) -> Result<Pool<Postgres>> {
        match self.auth_type {
            AuthType::Postgres => match self.simple_connect().await {
                Ok(pool) => {
                    metric_tracker::start_tm(app_name, pool.clone()).await;
                    Ok(pool)
                }
                Err(err) => Err(err),
            },
            AuthType::Iam => {
                let pool = iam_auth_pool::connect_tm(self).await?;
                metric_tracker::start_tm(app_name, pool.clone()).await;
                Ok(pool)
            }
        }
    }

    async fn simple_connect(&self) -> Result<Pool<Postgres>> {
        let connect_options = self
            .url
            .as_ref()
            .ok_or_else(|| Error::InvalidConfiguration("url is required".to_string()))?
            .parse()?;

        let pool = self.pool_options().connect_with(connect_options).await?;
        Ok(pool)
    }

    pub fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new().max_connections(self.max_connections)
    }
}
