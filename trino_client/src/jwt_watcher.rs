use crate::error::{Error, Result};
use crate::settings::Settings;
use crate::{build_inner_client, InnerClient};
use notify::{Config, PollWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

/// Watches a JWT token file on disk and pushes a freshly-built Trino client
/// through the provided `watch::Sender` whenever the file changes.
///
/// The watcher is owned by a `PollWatcher` returned from [`JwtWatcher::start`];
/// keeping the `PollWatcher` alive keeps the internal polling thread running.
/// Dropping it stops polling.
pub(crate) struct JwtWatcher {
    settings: Settings,
    updater: watch::Sender<InnerClient>,
}

impl JwtWatcher {
    /// Build the watcher and ensure the token file is readable up front.
    pub(crate) fn new(settings: Settings, updater: watch::Sender<InnerClient>) -> Result<Self> {
        let jwt_watcher = Self { settings, updater };
        // Ensure we can read the token file before we continue.
        let _ = jwt_watcher.settings.resolve_jwt_token()?;
        Ok(jwt_watcher)
    }

    /// Construct and start the `PollWatcher`. The returned handle owns the
    /// internal polling thread; keep it alive for as long as you want refresh
    /// events to be processed.
    pub(crate) fn start(
        self,
        token_path: &Path,
        refresh_interval: Duration,
    ) -> Result<PollWatcher> {
        let config = Config::default()
            .with_poll_interval(refresh_interval)
            .with_compare_contents(true);
        let mut watcher = PollWatcher::new(self, config)?;
        watcher.watch(token_path, RecursiveMode::NonRecursive)?;
        Ok(watcher)
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

    #[test]
    fn from_settings_errors_without_tokio_runtime() {
        // Use a fresh thread that has no tokio runtime context. The current
        // process's runtime (if any from another test) would not be visible
        // here.
        let handle = std::thread::spawn(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("token.jwt");
            std::fs::write(&path, b"some-token").unwrap();
            let settings = base_settings(Some(AuthSettings::JwtFile {
                path,
                refresh_interval: Duration::from_millis(50),
            }));
            Client::from_settings(&settings)
        });
        let result = handle.join().unwrap();
        let err = result.err().expect("expected error");
        assert!(matches!(err, Error::NoTokioRuntime), "got: {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_file_change_refreshes_inner_client() {
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

        // Dropping the client must tear down the watcher task; this is
        // exercised by the implicit `drop(client)` at scope exit.
    }
}
