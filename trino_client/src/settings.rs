use serde::Deserialize;
use std::path::PathBuf;

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
    /// Path to a PEM-encoded CA certificate to trust in addition to the
    /// platform roots.
    #[serde(default)]
    pub ca_cert_path: Option<PathBuf>,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn deserializes_tls_options() {
        let s: Settings = serde_json::from_value(json!({
            "host": "trino.example.com",
            "port": 443,
            "user": "alice",
            "secure": true,
            "ca_cert_path": "/etc/ssl/trino-ca.pem",
            "insecure_skip_tls_verify": false,
        }))
        .unwrap();
        assert!(s.secure);
        assert_eq!(
            s.ca_cert_path.unwrap().to_str().unwrap(),
            "/etc/ssl/trino-ca.pem"
        );
        assert!(!s.insecure_skip_tls_verify);
    }
}
