pub use trino_rust_client;
pub use trino_rust_client::Trino as TrinoFromRow;

mod builder;
mod error;
mod jwt_watcher;
mod settings;
mod statement;

pub use builder::ClientBuilder;
pub use error::{Error, Result};
pub use settings::{AuthSettings, Settings};
pub use statement::{Param, Statement, TypedStatement};

pub use jwt_watcher::JwtWatcherError;

use jwt_watcher::JwtWatcher;
use notify::PollWatcher;
use std::sync::Arc;
use tokio::sync::watch;
use trino_rust_client::auth::Auth;
use trino_rust_client::ClientBuilder as UpstreamClientBuilder;

type TrinoClient = Arc<trino_rust_client::Client>;
type TrinoClientSender = watch::Sender<TrinoClient>;
type TrinoClientReceiver = watch::Receiver<TrinoClient>;
type TrinoClientSendError = watch::error::SendError<TrinoClient>;

pub trait SqlStatement {
    fn to_statement(&self) -> Statement;
}

impl<T: SqlStatement + ?Sized> SqlStatement for &T {
    fn to_statement(&self) -> Statement {
        (*self).to_statement()
    }
}

pub trait SqlQuery: SqlStatement {
    type Row;
}

impl<T: SqlQuery + ?Sized> SqlQuery for &T {
    type Row = T::Row;
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    // Declared first so it drops first: dropping the `PollWatcher` signals its
    // internal polling thread to stop *before* the watch::Receiver disappears,
    // minimizing the race where a tick observes a closed channel.
    _watcher: Option<PollWatcher>,
    inner_rx: TrinoClientReceiver,
}

impl Client {
    /// Build a client from settings. If `settings.auth` is
    /// [`AuthSettings::JwtFile`], a background `notify::PollWatcher` is started
    /// that watches the token file and swaps the inner Trino client each time
    /// the file changes. The watcher's polling thread stops automatically when
    /// the last clone of the returned `Client` is dropped.
    ///
    /// This is a fully synchronous constructor — it does not require a tokio
    /// runtime to be active.
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let token = settings.resolve_jwt_token()?;
        let initial = build_inner_client(settings, token.as_deref())?;
        let (updater, inner_rx) = watch::channel(initial);

        let watcher = match &settings.auth {
            Some(AuthSettings::JwtFile {
                path,
                refresh_interval,
            }) => Some(JwtWatcher::new(settings.clone(), updater)?.start(path, *refresh_interval)?),
            _ => None,
        };

        Ok(Self {
            inner: Arc::new(ClientInner {
                _watcher: watcher,
                inner_rx,
            }),
        })
    }

    /// Returns the current inner client. JWT refreshes swap the value behind
    /// this `Arc`, so callers should call `inner()` per request rather than
    /// caching the returned `Arc` long-term.
    fn inner(&self) -> TrinoClient {
        self.inner.inner_rx.borrow().clone()
    }

    pub async fn execute<S: SqlStatement>(&self, sql_statement: S) -> Result<()> {
        self.execute_raw(sql_statement.to_statement().render()?)
            .await
    }

    pub async fn get_all<S: SqlQuery>(&self, sql_query: S) -> Result<Vec<S::Row>>
    where
        S::Row: trino_rust_client::Trino + 'static,
        for<'de> S::Row: serde::Deserialize<'de> + serde::Serialize,
    {
        self.get_all_raw(sql_query.to_statement().render()?).await
    }

    pub async fn execute_raw(&self, sql: impl Into<String>) -> Result<()> {
        self.inner().execute(sql.into()).await?;
        Ok(())
    }

    pub async fn get_all_raw<T>(&self, sql: impl Into<String>) -> Result<Vec<T>>
    where
        T: trino_rust_client::Trino + 'static,
        for<'de> T: serde::Deserialize<'de> + serde::Serialize,
    {
        match self.inner().get_all::<T>(sql.into()).await {
            Ok(ds) => Ok(ds.into_vec()),
            Err(trino_rust_client::error::Error::EmptyData) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }
}

pub(crate) fn build_inner_client(
    settings: &Settings,
    jwt_override: Option<&str>,
) -> std::result::Result<TrinoClient, trino_rust_client::error::Error> {
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
    match &settings.auth {
        Some(AuthSettings::Basic { username, password }) => {
            builder = builder.auth(Auth::Basic(username.clone(), password.clone()));
        }
        Some(AuthSettings::Jwt { token }) => {
            builder = builder.auth(Auth::Jwt(token.clone()));
        }
        Some(AuthSettings::JwtFile { .. }) => {
            if let Some(token) = jwt_override {
                builder = builder.auth(Auth::Jwt(token.to_owned()));
            }
        }
        None => {}
    }

    let inner = builder.build()?;
    Ok(Arc::new(inner))
}
