use crate::error::Result;
use crate::settings::{AuthSettings, Settings};
use crate::Client;

pub struct ClientBuilder {
    host: String,
    port: u16,
    user: String,
    catalog: Option<String>,
    schema: Option<String>,
    secure: bool,
    insecure_skip_tls_verify: bool,
    auth: Option<AuthSettings>,
}

impl ClientBuilder {
    pub fn new(host: impl Into<String>, port: u16, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            catalog: None,
            schema: None,
            secure: false,
            insecure_skip_tls_verify: false,
            auth: None,
        }
    }

    pub fn catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    pub fn insecure_skip_tls_verify(mut self, skip: bool) -> Self {
        self.insecure_skip_tls_verify = skip;
        self
    }

    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(AuthSettings::Basic {
            username: username.into(),
            password: Some(password.into()),
        });
        self
    }

    pub fn jwt_auth(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(AuthSettings::Jwt {
            token: token.into(),
        });
        self
    }

    pub fn build(self) -> Result<Client> {
        let settings = Settings {
            host: self.host,
            port: self.port,
            user: self.user,
            catalog: self.catalog,
            schema: self.schema,
            secure: self.secure,
            insecure_skip_tls_verify: self.insecure_skip_tls_verify,
            auth: self.auth,
        };
        Client::from_settings(&settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_minimal_client() {
        ClientBuilder::new("localhost", 8080, "admin")
            .build()
            .expect("minimal builder should build");
    }

    #[test]
    fn builds_with_all_options() {
        ClientBuilder::new("trino.example.com", 443, "alice")
            .catalog("memory")
            .schema("default")
            .secure(true)
            .basic_auth("alice", "hunter2")
            .build()
            .expect("full builder should build");
    }

    #[test]
    fn builds_with_jwt_auth() {
        ClientBuilder::new("trino.example.com", 443, "alice")
            .secure(true)
            .jwt_auth("ey.fake.token")
            .build()
            .expect("jwt builder should build");
    }

    #[test]
    fn jwt_overrides_basic_auth() {
        let _ = ClientBuilder::new("trino.example.com", 443, "alice")
            .secure(true)
            .basic_auth("alice", "hunter2")
            .jwt_auth("ey.fake.token")
            .build()
            .expect("last auth wins");
    }
}
