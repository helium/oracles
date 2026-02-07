use crate::{Error, Result, Settings};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    CommitTableRequest, RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Response from the REST catalog's config endpoint.
#[derive(serde::Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
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
    /// Optional auth token for API calls.
    auth_token: Option<String>,
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

        let mut http_request = self.client.post(&url).json(request);

        if let Some(ref token) = self.auth_token {
            http_request = http_request.bearer_auth(token);
        }

        let response = http_request
            .send()
            .await
            .map_err(|e| Error::Catalog(format!("commit request failed: {e}")))?;

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

        let mut config_url = format!("{}/v1/config", settings.catalog_uri);
        if let Some(ref warehouse) = settings.warehouse {
            config_url = format!("{config_url}?warehouse={warehouse}");
        }

        let mut request = client.get(&config_url);
        if let Some(ref token) = settings.auth_token {
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
            auth_token: settings.auth_token.clone(),
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

    /// Send a commit table request directly to the REST catalog API.
    ///
    /// This bypasses `TableCommit` (whose builder is `pub(crate)` in iceberg 0.8)
    /// by constructing and sending a `CommitTableRequest` via HTTP POST.
    pub(crate) async fn commit_table_request(&self, request: &CommitTableRequest) -> Result<()> {
        self.endpoint.commit_table(request).await
    }
}

impl AsRef<RestCatalog> for Catalog {
    fn as_ref(&self) -> &RestCatalog {
        &self.inner
    }
}
