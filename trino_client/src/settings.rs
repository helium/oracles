use crate::error::Result;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub host: String,
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    /// Use HTTPS for the connection. Required by the upstream client when
    /// `auth` is set unless the server is on the same host.
    #[serde(default)]
    pub secure: bool,
    /// Skip TLS certificate verification. For local development only.
    #[serde(default)]
    pub insecure_skip_tls_verify: bool,
    /// Authentication credentials. None means anonymous.
    #[serde(default)]
    pub auth: Option<AuthSettings>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthSettings {
    Basic {
        username: String,
        #[serde(default)]
        password: Option<String>,
    },
    Jwt {
        token: String,
    },
    JwtFile {
        path: PathBuf,
        #[serde(with = "humantime_serde", default = "default_jwt_refresh_interval")]
        refresh_interval: Duration,
    },
}

fn default_jwt_refresh_interval() -> Duration {
    Duration::from_secs(60)
}

impl Settings {
    /// Resolve the JWT token to send with requests.
    ///
    /// - `Jwt { token }`: returns the literal token.
    /// - `JwtFile { path, .. }`: reads the file, trims whitespace, and returns
    ///   `None` if the resulting string is empty.
    /// - `Basic` or `None`: returns `None`.
    pub fn resolve_jwt_token(&self) -> Result<Option<String>> {
        match &self.auth {
            Some(AuthSettings::Jwt { token }) => Ok(Some(token.clone())),
            Some(AuthSettings::JwtFile { path, .. }) => {
                let token = std::fs::read_to_string(path)?;
                let token = token.trim().to_owned();
                if token.is_empty() {
                    tracing::warn!(path = %path.display(), "jwt token file is empty");
                    Ok(None)
                } else {
                    tracing::info!(path = %path.display(), "jwt token loaded");
                    Ok(Some(token))
                }
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    fn write_token_file(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn settings_with_auth(auth: Option<AuthSettings>) -> Settings {
        Settings {
            host: "localhost".into(),
            port: 8080,
            user: "alice".into(),
            catalog: None,
            schema: None,
            secure: false,
            insecure_skip_tls_verify: false,
            auth,
        }
    }

    #[test]
    fn deserializes_minimal_settings() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 8080,
            "user": "alice",
        }))
        .unwrap();
        assert_eq!(s.host, "trino.example.com");
        assert_eq!(s.port, 8080);
        assert_eq!(s.user, "alice");
        assert!(s.catalog.is_none());
        assert!(s.auth.is_none());
        assert!(!s.secure);
    }

    #[test]
    fn deserializes_basic_auth() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "auth": { "type": "basic", "username": "alice", "password": "hunter2" },
        }))
        .unwrap();
        assert!(s.secure);
        match s.auth.unwrap() {
            AuthSettings::Basic { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password.as_deref(), Some("hunter2"));
            }
            _ => panic!("expected basic auth"),
        }
    }

    #[test]
    fn deserializes_basic_auth_without_password() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "auth": { "type": "basic", "username": "alice" },
        }))
        .unwrap();
        match s.auth.unwrap() {
            AuthSettings::Basic { username, password } => {
                assert_eq!(username, "alice");
                assert!(password.is_none());
            }
            _ => panic!("expected basic auth"),
        }
    }

    #[test]
    fn deserializes_jwt_auth() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "auth": { "type": "jwt", "token": "ey.fake.token" },
        }))
        .unwrap();
        match s.auth.unwrap() {
            AuthSettings::Jwt { token } => assert_eq!(token, "ey.fake.token"),
            _ => panic!("expected jwt auth"),
        }
    }

    #[test]
    fn deserializes_jwt_file_auth_default_interval() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "auth": { "type": "jwt_file", "path": "/var/run/trino.jwt" },
        }))
        .unwrap();
        match s.auth.unwrap() {
            AuthSettings::JwtFile {
                path,
                refresh_interval,
            } => {
                assert_eq!(path, PathBuf::from("/var/run/trino.jwt"));
                assert_eq!(refresh_interval, Duration::from_secs(60));
            }
            _ => panic!("expected jwt_file auth"),
        }
    }

    #[test]
    fn deserializes_jwt_file_auth_custom_interval() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "auth": {
                "type": "jwt_file",
                "path": "/var/run/trino.jwt",
                "refresh_interval": "30s",
            },
        }))
        .unwrap();
        match s.auth.unwrap() {
            AuthSettings::JwtFile {
                refresh_interval, ..
            } => {
                assert_eq!(refresh_interval, Duration::from_secs(30));
            }
            _ => panic!("expected jwt_file auth"),
        }
    }

    #[test]
    fn deserializes_tls_options() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "insecure_skip_tls_verify": false,
        }))
        .unwrap();
        assert!(s.secure);
        assert!(!s.insecure_skip_tls_verify);
    }

    #[test]
    fn resolve_jwt_no_auth_returns_none() {
        let s = settings_with_auth(None);
        assert_eq!(s.resolve_jwt_token().unwrap(), None);
    }

    #[test]
    fn resolve_jwt_basic_returns_none() {
        let s = settings_with_auth(Some(AuthSettings::Basic {
            username: "alice".into(),
            password: Some("hunter2".into()),
        }));
        assert_eq!(s.resolve_jwt_token().unwrap(), None);
    }

    #[test]
    fn resolve_jwt_static_returns_token() {
        let s = settings_with_auth(Some(AuthSettings::Jwt {
            token: "ey.static".into(),
        }));
        assert_eq!(s.resolve_jwt_token().unwrap(), Some("ey.static".into()));
    }

    #[test]
    fn resolve_jwt_file_returns_token() {
        let f = write_token_file("my-secret-token");
        let s = settings_with_auth(Some(AuthSettings::JwtFile {
            path: f.path().to_path_buf(),
            refresh_interval: Duration::from_secs(60),
        }));
        assert_eq!(
            s.resolve_jwt_token().unwrap(),
            Some("my-secret-token".into())
        );
    }

    #[test]
    fn resolve_jwt_file_trims_whitespace() {
        let f = write_token_file("  my-secret-token\n");
        let s = settings_with_auth(Some(AuthSettings::JwtFile {
            path: f.path().to_path_buf(),
            refresh_interval: Duration::from_secs(60),
        }));
        assert_eq!(
            s.resolve_jwt_token().unwrap(),
            Some("my-secret-token".into())
        );
    }

    #[test]
    fn resolve_jwt_file_empty_returns_none() {
        let f = write_token_file("");
        let s = settings_with_auth(Some(AuthSettings::JwtFile {
            path: f.path().to_path_buf(),
            refresh_interval: Duration::from_secs(60),
        }));
        assert_eq!(s.resolve_jwt_token().unwrap(), None);
    }

    #[test]
    fn resolve_jwt_file_whitespace_only_returns_none() {
        let f = write_token_file("   \n\t  ");
        let s = settings_with_auth(Some(AuthSettings::JwtFile {
            path: f.path().to_path_buf(),
            refresh_interval: Duration::from_secs(60),
        }));
        assert_eq!(s.resolve_jwt_token().unwrap(), None);
    }

    #[test]
    fn resolve_jwt_file_missing_returns_err() {
        let s = settings_with_auth(Some(AuthSettings::JwtFile {
            path: PathBuf::from("/nonexistent/path/to/token.jwt"),
            refresh_interval: Duration::from_secs(60),
        }));
        assert!(s.resolve_jwt_token().is_err());
    }
}
