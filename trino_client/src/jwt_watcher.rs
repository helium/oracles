use crate::error::{Error, Result};
use crate::settings::Settings;
use crate::{build_inner_client, InnerClient};
use notify::Watcher;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

/// Watches a JWT token file on disk and pushes a freshly-built Trino client
/// through the provided `watch::Sender` whenever the file changes.
pub struct JwtWatcher {
    settings: Settings,
    updater: watch::Sender<InnerClient>,
}

impl JwtWatcher {
    pub fn new(settings: Settings, updater: watch::Sender<InnerClient>) -> Result<Self> {
        let jwt_watcher = Self { settings, updater };
        // Ensure we can read the token file before we continue.
        let _ = jwt_watcher.settings.resolve_jwt_token()?;
        Ok(jwt_watcher)
    }

    fn create_and_send_new_inner(&self) -> Result<()> {
        let jwt_token = self.settings.resolve_jwt_token()?;
        let new_inner = build_inner_client(&self.settings, jwt_token.as_deref())?;
        self.updater
            .send(new_inner)
            .map_err(|e| Error::WatchSend(e.to_string()))?;
        tracing::info!("created new inner trino client");
        Ok(())
    }

    pub fn make_watcher(
        self,
        token_path: &Path,
        refresh_interval: Duration,
        shutdown: triggered::Listener,
    ) -> Result<impl Future<Output = Result<()>>> {
        use notify::{Config, PollWatcher, RecursiveMode};

        let config = Config::default()
            .with_poll_interval(refresh_interval)
            .with_compare_contents(true);
        let mut watcher = PollWatcher::new(self, config)?;

        watcher.watch(token_path, RecursiveMode::NonRecursive)?;

        Ok(async move {
            let _watcher = watcher;
            shutdown.await;
            tracing::info!("jwt watcher shutting down");
            Ok(())
        })
    }
}

impl notify::EventHandler for JwtWatcher {
    fn handle_event(&mut self, res: notify::Result<notify::Event>) {
        let event = match res {
            Ok(event) => event,
            Err(err) => {
                tracing::error!(?err, "jwt watcher received error event");
                return;
            }
        };

        tracing::debug!(?event, "jwt watcher event");
        let should_handle = event.kind.is_modify() || event.kind.is_create();
        if !should_handle {
            tracing::debug!("ignoring jwt watcher event");
            return;
        }

        if let Err(err) = self.create_and_send_new_inner() {
            tracing::error!(?err, "failed to refresh trino client");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{AuthSettings, Client, Error, Settings};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    fn base_settings(auth: Option<AuthSettings>) -> Settings {
        // `trino-rust-client` rejects JWT auth with `secure: false`, so default
        // to HTTPS for these tests. None of the tests actually issue requests.
        let secure = matches!(auth, Some(AuthSettings::JwtFile { .. }));
        Settings {
            host: "localhost".into(),
            port: 8080,
            user: "alice".into(),
            catalog: None,
            schema: None,
            secure,
            insecure_skip_tls_verify: true,
            auth,
        }
    }

    #[tokio::test]
    async fn watch_jwt_errors_without_jwt_file_auth() {
        let client = Client::from_settings(&base_settings(None)).unwrap();
        let (_trigger, listener) = triggered::trigger();
        let err = client.watch_jwt(listener).err().expect("expected error");
        assert!(matches!(err, Error::JwtWatchUnsupported));
    }

    #[tokio::test]
    async fn from_settings_fails_when_jwt_file_path_missing() {
        let settings = base_settings(Some(AuthSettings::JwtFile {
            path: PathBuf::from("/nonexistent/path/to/token.jwt"),
            refresh_interval: Duration::from_millis(50),
        }));
        let err = Client::from_settings(&settings)
            .err()
            .expect("expected error");
        assert!(matches!(err, Error::Io(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_jwt_refreshes_on_file_change() {
        let mut token_file = tempfile::NamedTempFile::new().unwrap();
        token_file.write_all(b"initial-token").unwrap();
        token_file.flush().unwrap();

        let settings = base_settings(Some(AuthSettings::JwtFile {
            path: token_file.path().to_path_buf(),
            refresh_interval: Duration::from_millis(50),
        }));

        let client = Client::from_settings(&settings).unwrap();
        let initial = client.inner();
        let initial_ptr = Arc::as_ptr(&initial);

        let (trigger, listener) = triggered::trigger();
        let watcher_fut = client.watch_jwt(listener).unwrap();
        let watcher_handle = tokio::spawn(watcher_fut);

        // Give the watcher time to settle, then rewrite the token file.
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(token_file.path(), b"refreshed-token").unwrap();

        // Poll for up to ~2s for the Arc to be swapped.
        let mut swapped = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let current = client.inner();
            if !Arc::ptr_eq(&initial, &current) {
                swapped = true;
                assert_ne!(initial_ptr, Arc::as_ptr(&current));
                break;
            }
        }
        assert!(swapped, "watcher did not refresh the inner client");

        trigger.trigger();
        watcher_handle.await.unwrap().unwrap();
    }
}
