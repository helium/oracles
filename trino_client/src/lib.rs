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

use jwt_watcher::JwtWatcher;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use trino_rust_client::auth::Auth;
use trino_rust_client::ClientBuilder as UpstreamClientBuilder;

pub type InnerClient = Arc<trino_rust_client::Client>;

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
    inner_rx: watch::Receiver<InnerClient>,
    settings: Settings,
    // Aborted on Drop. `None` for non-`JwtFile` auth.
    watcher_task: Option<JoinHandle<()>>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        if let Some(handle) = self.watcher_task.take() {
            handle.abort();
        }
    }
}

impl Client {
    /// Build a client from settings. If `settings.auth` is
    /// [`AuthSettings::JwtFile`], a background task is spawned on the current
    /// tokio runtime to watch the token file and swap the inner Trino client
    /// each time the file changes. The task is automatically aborted when the
    /// last clone of the returned `Client` is dropped.
    ///
    /// Returns [`Error::NoTokioRuntime`] when called outside any tokio runtime
    /// with `JwtFile` auth.
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let token = settings.resolve_jwt_token()?;
        let initial = build_inner_client(settings, token.as_deref())?;
        let (updater, inner_rx) = watch::channel(initial);

        let watcher_task = match &settings.auth {
            Some(AuthSettings::JwtFile {
                path,
                refresh_interval,
            }) => {
                // Refuse to silently no-op outside a runtime.
                let rt =
                    tokio::runtime::Handle::try_current().map_err(|_| Error::NoTokioRuntime)?;

                let watcher = JwtWatcher::new(settings.clone(), updater)?;
                let poll_watcher = watcher.start(path, *refresh_interval)?;

                // The spawned task's only job is to keep `poll_watcher` alive.
                // Aborting it via the JoinHandle on Drop releases the watcher,
                // which stops its internal polling thread.
                Some(rt.spawn(async move {
                    let _watcher = poll_watcher;
                    std::future::pending::<()>().await;
                }))
            }
            _ => {
                // No watcher: keep `updater` alive on `Client` only via
                // `inner_rx` — but `watch::Receiver` doesn't hold the sender
                // alive. We don't need to refresh the inner client, so just
                // drop the sender. The receiver still holds the last value.
                drop(updater);
                None
            }
        };

        Ok(Self {
            inner: Arc::new(ClientInner {
                inner_rx,
                settings: settings.clone(),
                watcher_task,
            }),
        })
    }

    /// Returns the current inner client. JWT refreshes swap the value behind
    /// this `Arc`, so callers should call `inner()` per request rather than
    /// caching the returned `Arc` long-term.
    pub fn inner(&self) -> InnerClient {
        self.inner.inner_rx.borrow().clone()
    }

    pub fn settings(&self) -> &Settings {
        &self.inner.settings
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
) -> Result<InnerClient> {
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

    let inner = builder
        .build()
        .map_err(|e| Error::Build(format!("{e:?}")))?;
    Ok(Arc::new(inner))
}
