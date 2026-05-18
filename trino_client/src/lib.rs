pub use trino_rust_client;
pub use trino_rust_client::Trino as TrinoFromRow;

mod builder;
mod error;
mod settings;
mod statement;

pub use builder::ClientBuilder;
pub use error::{Error, Result};
pub use settings::{AuthSettings, Settings};
pub use statement::{Param, Statement, TypedStatement};

use trino_rust_client::auth::Auth;
use trino_rust_client::ssl::Ssl;
use trino_rust_client::ClientBuilder as UpstreamClientBuilder;

pub trait SqlStatement {
    fn to_statement(&self) -> Statement;
}

pub trait SqlQuery: SqlStatement {
    type Row;
}

pub struct Client {
    inner: trino_rust_client::Client,
}

impl Client {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let mut builder = UpstreamClientBuilder::new(&settings.user, &settings.host)
            .port(settings.port)
            .secure(settings.secure);

        if let Some(catalog) = &settings.catalog {
            builder = builder.catalog(catalog);
        }
        if let Some(schema) = &settings.schema {
            builder = builder.schema(schema);
        }
        if settings.insecure_skip_tls_verify {
            builder = builder.no_verify(true);
        }
        if let Some(path) = &settings.ca_cert_path {
            let cert = Ssl::read_pem(path)
                .map_err(|e| Error::Build(format!("read ca cert {path:?}: {e:?}")))?;
            builder = builder.ssl(Ssl {
                root_cert: Some(cert),
            });
        }
        if let Some(auth) = &settings.auth {
            builder = builder.auth(match auth {
                AuthSettings::Basic { username, password } => {
                    Auth::Basic(username.clone(), password.clone())
                }
                AuthSettings::Jwt { token } => Auth::Jwt(token.clone()),
            });
        }

        let inner = builder
            .build()
            .map_err(|e| Error::Build(format!("{e:?}")))?;
        Ok(Self { inner })
    }

    pub fn from_inner(inner: trino_rust_client::Client) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &trino_rust_client::Client {
        &self.inner
    }

    pub async fn execute<S: SqlStatement>(&self, sql_statement: &S) -> Result<()> {
        self.execute_raw(sql_statement.to_statement().render()?)
            .await
    }

    pub async fn get_all<S: SqlQuery>(&self, sql_query: &S) -> Result<Vec<S::Row>>
    where
        S::Row: trino_rust_client::Trino + 'static,
        for<'de> S::Row: serde::Deserialize<'de> + serde::Serialize,
    {
        self.get_all_raw(sql_query.to_statement().render()?).await
    }

    pub async fn execute_raw(&self, sql: impl Into<String>) -> Result<()> {
        self.inner.execute(sql.into()).await?;
        Ok(())
    }

    pub async fn get_all_raw<T>(&self, sql: impl Into<String>) -> Result<Vec<T>>
    where
        T: trino_rust_client::Trino + 'static,
        for<'de> T: serde::Deserialize<'de> + serde::Serialize,
    {
        match self.inner.get_all::<T>(sql.into()).await {
            Ok(ds) => Ok(ds.into_vec()),
            Err(trino_rust_client::error::Error::EmptyData) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }
}
