use crate::{Error, IcebergTable, Result, Settings, TableCreator, TableDefinition};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    CommitTableRequest, RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// OAuth2 token response from the token endpoint.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Response from the REST catalog's config endpoint.
#[derive(serde::Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
}

/// Authentication strategy for direct REST API calls.
#[derive(Clone)]
enum EndpointAuth {
    None,
    Token(String),
    OAuth2 {
        token_endpoint: String,
        credential: String,
        extra_params: HashMap<String, String>,
        cached_token: Arc<Mutex<Option<String>>>,
    },
}

impl EndpointAuth {
    /// Build from settings, determining the auth strategy.
    fn from_settings(settings: &Settings) -> Self {
        if let Some(ref credential) = settings.auth.credential {
            let token_endpoint = settings
                .auth
                .oauth2_server_uri
                .clone()
                .unwrap_or_else(|| format!("{}/v1/oauth/tokens", settings.catalog_uri));

            let mut extra_params = HashMap::new();
            if let Some(ref scope) = settings.auth.scope {
                extra_params.insert("scope".to_string(), scope.clone());
            }
            if let Some(ref audience) = settings.auth.audience {
                extra_params.insert("audience".to_string(), audience.clone());
            }
            if let Some(ref resource) = settings.auth.resource {
                extra_params.insert("resource".to_string(), resource.clone());
            }

            Self::OAuth2 {
                token_endpoint,
                credential: credential.clone(),
                extra_params,
                cached_token: Arc::new(Mutex::new(None)),
            }
        } else if let Some(ref token) = settings.auth.token {
            Self::Token(token.clone())
        } else {
            Self::None
        }
    }

    /// Fetch a fresh OAuth2 token from the token endpoint.
    async fn fetch_token(
        client: &reqwest::Client,
        token_endpoint: &str,
        credential: &str,
        extra_params: &HashMap<String, String>,
    ) -> Result<String> {
        let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

        let mut params = vec![
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ];
        let extra: Vec<(&str, &str)> = extra_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        params.extend(extra);

        let response = client
            .post(token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| Error::Catalog(format!("OAuth2 token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Catalog(format!(
                "OAuth2 token request returned {status}: {body}"
            )));
        }

        response
            .json::<TokenResponse>()
            .await
            .map(|r| r.access_token)
            .map_err(|e| Error::Catalog(format!("failed to parse OAuth2 token response: {e}")))
    }

    /// Get a valid token, using the cache if available.
    async fn get_token(&self, client: &reqwest::Client) -> Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::Token(token) => Ok(Some(token.clone())),
            Self::OAuth2 {
                token_endpoint,
                credential,
                extra_params,
                cached_token,
            } => {
                let mut guard = cached_token.lock().await;
                if let Some(ref token) = *guard {
                    return Ok(Some(token.clone()));
                }
                let token =
                    Self::fetch_token(client, token_endpoint, credential, extra_params).await?;
                *guard = Some(token.clone());
                Ok(Some(token))
            }
        }
    }

    /// Invalidate the cached token (called on 401 to trigger a refresh).
    async fn invalidate(&self) {
        if let Self::OAuth2 { cached_token, .. } = self {
            let mut guard = cached_token.lock().await;
            *guard = None;
        }
    }
}

/// Resolved endpoint configuration for making direct REST API calls.
#[derive(Clone)]
struct RestEndpoint {
    /// The base URL for the REST catalog (may be overridden by server config).
    uri: String,
    /// The optional prefix from the REST catalog config.
    prefix: Option<String>,
    /// HTTP client for direct API calls.
    client: reqwest::Client,
    /// Authentication strategy.
    auth: EndpointAuth,
}

impl RestEndpoint {
    /// Build the URL for a table endpoint.
    fn table_url(&self, table_ident: &TableIdent) -> String {
        let namespace = table_ident.namespace.to_url_string();
        let parts: Vec<&str> = [self.uri.as_str(), "v1"]
            .into_iter()
            .chain(self.prefix.as_deref())
            .chain(["namespaces", &namespace, "tables", &table_ident.name])
            .collect();
        parts.join("/")
    }

    /// Send a commit table request directly to the REST catalog.
    async fn commit_table(&self, request: &CommitTableRequest) -> Result<()> {
        let url = request
            .identifier
            .as_ref()
            .map(|ident| self.table_url(ident))
            .ok_or_else(|| Error::Catalog("table identifier required for commit".into()))?;

        let response = self.send_commit(&url, request).await?;

        match response.status().as_u16() {
            200 => return Ok(()),
            401 => {
                // Invalidate cached token and retry once
                self.auth.invalidate().await;
                let retry_response = self.send_commit(&url, request).await?;
                return Self::handle_commit_response(retry_response).await;
            }
            _ => {}
        }

        Self::handle_commit_response(response).await
    }

    /// Send the HTTP POST for a commit request, attaching auth.
    async fn send_commit(
        &self,
        url: &str,
        request: &CommitTableRequest,
    ) -> Result<reqwest::Response> {
        let mut http_request = self.client.post(url).json(request);

        if let Some(token) = self.auth.get_token(&self.client).await? {
            http_request = http_request.bearer_auth(token);
        }

        http_request
            .send()
            .await
            .map_err(|e| Error::Catalog(format!("commit request failed: {e}")))
    }

    /// Map an HTTP response to a Result.
    async fn handle_commit_response(response: reqwest::Response) -> Result<()> {
        match response.status().as_u16() {
            200 => Ok(()),
            409 => Err(Error::Catalog(
                "commit conflict: one or more requirements failed".into(),
            )),
            404 => Err(Error::Catalog("table not found".into())),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(Error::Catalog(format!(
                    "unexpected status {status}: {body}"
                )))
            }
        }
    }
}

/// A wrapper around `RestCatalog` that is `Clone` and shareable across tasks.
///
/// Since `RestCatalog` from the iceberg crate is not `Clone`, this struct wraps it
/// in an `Arc` to enable sharing a single catalog connection across multiple
/// components like `IcebergTable` and `TableCreator`.
///
/// Also provides direct REST API access for branch operations that cannot go
/// through the `Transaction` API due to `TableCommit::builder().build()` being
/// `pub(crate)` in iceberg 0.8.
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<RestCatalog>,
    endpoint: RestEndpoint,
}

impl Catalog {
    /// Connect to an Iceberg REST catalog using the provided settings.
    pub async fn connect(settings: &Settings) -> Result<Self> {
        let mut config = HashMap::new();
        config.insert(
            REST_CATALOG_PROP_URI.to_string(),
            settings.catalog_uri.clone(),
        );
        if let Some(ref warehouse) = settings.warehouse {
            config.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }
        config.extend(settings.auth.props());
        config.extend(settings.s3.props());
        config.extend(settings.properties.clone());

        let catalog = RestCatalogBuilder::default()
            .load(&settings.catalog_name, config)
            .await
            .map_err(Error::Iceberg)?;

        let endpoint = Self::resolve_endpoint(settings).await?;

        Ok(Self {
            inner: Arc::new(catalog),
            endpoint,
        })
    }

    /// Resolve the REST endpoint by fetching the catalog config from the server.
    async fn resolve_endpoint(settings: &Settings) -> Result<RestEndpoint> {
        let client = reqwest::Client::new();
        let auth = EndpointAuth::from_settings(settings);

        let mut config_url = format!("{}/v1/config", settings.catalog_uri);
        if let Some(ref warehouse) = settings.warehouse {
            config_url = format!("{config_url}?warehouse={warehouse}");
        }

        let mut request = client.get(&config_url);
        if let Some(token) = auth.get_token(&client).await? {
            request = request.bearer_auth(token);
        }

        // Fetch server config to discover any URI override or prefix
        let (uri, prefix) = match request.send().await {
            Ok(response) if response.status().is_success() => {
                let config: CatalogConfigResponse = response
                    .json()
                    .await
                    .map_err(|e| Error::Catalog(format!("failed to parse config: {e}")))?;

                let uri = config
                    .overrides
                    .get("uri")
                    .cloned()
                    .unwrap_or_else(|| settings.catalog_uri.clone());

                // Prefix can come from defaults or overrides (overrides take precedence)
                let prefix = config
                    .overrides
                    .get("prefix")
                    .or_else(|| config.defaults.get("prefix"))
                    .cloned();

                (uri, prefix)
            }
            _ => (settings.catalog_uri.clone(), None),
        };

        Ok(RestEndpoint {
            uri,
            prefix,
            client,
            auth,
        })
    }

    /// Check if a table exists in the given namespace.
    pub async fn table_exists(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<bool> {
        let namespace_ident = NamespaceIdent::new(namespace.into());
        let table_ident = TableIdent::new(namespace_ident, table_name.into());

        self.inner
            .table_exists(&table_ident)
            .await
            .map_err(Error::Iceberg)
    }

    /// Load a table from the catalog.
    pub async fn load_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Table> {
        let namespace = namespace.into();
        let table_name = table_name.into();

        let namespace_ident = NamespaceIdent::new(namespace.clone());
        let table_ident = TableIdent::new(namespace_ident, table_name.clone());

        self.inner
            .load_table(&table_ident)
            .await
            .map_err(|e| match e.kind() {
                iceberg::ErrorKind::DataInvalid => Error::TableNotFound {
                    namespace,
                    table: table_name,
                },
                _ => Error::Iceberg(e),
            })
    }

    /// Create a namespace if it does not already exist.
    pub async fn create_namespace_if_not_exists(&self, namespace: impl Into<String>) -> Result<()> {
        let namespace_ident = NamespaceIdent::new(namespace.into());
        let exists = self
            .inner
            .namespace_exists(&namespace_ident)
            .await
            .map_err(Error::Iceberg)?;
        if !exists {
            self.inner
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .map_err(Error::Iceberg)?;
        }
        Ok(())
    }

    /// Create a table if it does not already exist.
    pub async fn create_table_if_not_exists<T>(
        &self,
        table_def: TableDefinition,
    ) -> Result<IcebergTable<T>> {
        TableCreator::new(self.clone())
            .create_table_if_not_exists(table_def)
            .await
    }

    /// Send a commit table request directly to the REST catalog API.
    ///
    /// This bypasses `TableCommit` (whose builder is `pub(crate)` in iceberg 0.8)
    /// by constructing and sending a `CommitTableRequest` via HTTP POST.
    pub(crate) async fn commit_table_request(&self, request: &CommitTableRequest) -> Result<()> {
        self.endpoint.commit_table(request).await
    }

    /// List all namespaces in the catalog.
    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>> {
        self.inner
            .list_namespaces(None)
            .await
            .map_err(Error::Iceberg)
    }

    /// List all namespaces under a parent namespace.
    pub async fn list_namespaces_under(
        &self,
        parent: impl Into<String>,
    ) -> Result<Vec<NamespaceIdent>> {
        let parent_ident = NamespaceIdent::new(parent.into());
        self.inner
            .list_namespaces(Some(&parent_ident))
            .await
            .map_err(Error::Iceberg)
    }

    /// List all tables in a namespace.
    pub async fn list_tables(&self, namespace: impl Into<String>) -> Result<Vec<TableIdent>> {
        let namespace_ident = NamespaceIdent::new(namespace.into());
        self.inner
            .list_tables(&namespace_ident)
            .await
            .map_err(Error::Iceberg)
    }
}

impl AsRef<RestCatalog> for Catalog {
    fn as_ref(&self) -> &RestCatalog {
        &self.inner
    }
}
