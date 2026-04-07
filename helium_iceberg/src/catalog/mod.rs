//! Iceberg REST catalog client with automatic authentication and retry handling.
//!
//! This module provides the [`Catalog`] type, a shareable wrapper around the
//! Iceberg REST catalog that handles:
//!
//! - OAuth2 token acquisition and caching
//! - Proactive token refresh before expiration
//! - Automatic retry on 401 authentication errors
//! - Direct REST API access for operations not supported by the iceberg crate

mod auth;
mod rest_endpoint;

use rest_endpoint::RestEndpoint;

use crate::error::IntoHeliumIcebergError;
use crate::{Error, IcebergTable, Result, Settings, TableCreator, TableDefinition};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    CommitTableRequest, RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// Check if an iceberg error indicates an authentication failure (401 Unauthorized).
fn is_iceberg_auth_error(error: &iceberg::Error) -> bool {
    let msg = format!("{error:?}");
    msg.contains("401") || msg.contains("Unauthorized") || msg.contains("unauthorized")
}

/// A shareable Iceberg REST catalog client with automatic retry on auth errors.
///
/// Since `RestCatalog` from the iceberg crate is not `Clone`, this struct wraps it
/// in an `Arc` to enable sharing a single catalog connection across multiple
/// components like `IcebergTable` and `TableCreator`.
///
/// Implements `iceberg::Catalog` with automatic 401 retry logic - when an operation
/// fails with an authentication error, the cached token is invalidated and the
/// operation is retried once.
///
/// Also provides direct REST API access for branch operations that cannot go
/// through the `Transaction` API due to `TableCommit::builder().build()` being
/// `pub(crate)` in iceberg 0.8.
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<RestCatalog>,
    endpoint: RestEndpoint,
}

impl AsRef<RestCatalog> for Catalog {
    fn as_ref(&self) -> &RestCatalog {
        &self.inner
    }
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl Catalog {
    pub(crate) async fn with_auth<F, Fut, T>(&self, mut f: F) -> iceberg::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = iceberg::Result<T>>,
    {
        match f().await {
            Ok(result) => Ok(result),
            Err(e) if is_iceberg_auth_error(&e) => {
                tracing::warn!("auth error, invaliding token and retrying");
                self.endpoint.auth.invalidate().await;
                self.inner.invalidate_token().await?;
                f().await
            }
            Err(e) => Err(e),
        }
    }

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

        let rest_catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            }))
            .load(&settings.catalog_name, config)
            .await
            .map_err(Error::Iceberg)?;

        let endpoint = RestEndpoint::resolve(settings).await?;

        Ok(Self {
            inner: Arc::new(rest_catalog),
            endpoint,
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

        self.with_auth(|| self.inner.table_exists(&table_ident))
            .await
            .err_into()
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

        self.with_auth(|| self.inner.load_table(&table_ident))
            .await
            .map_err(|e: iceberg::Error| match e.kind() {
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
            .with_auth(|| self.inner.namespace_exists(&namespace_ident))
            .await?;

        if !exists {
            self.with_auth(|| {
                self.inner
                    .create_namespace(&namespace_ident, HashMap::new())
            })
            .await?;
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
        self.with_auth(|| self.inner.list_namespaces(None))
            .await
            .err_into()
    }

    /// List all namespaces under a parent namespace.
    pub async fn list_namespaces_under(
        &self,
        parent: impl Into<String>,
    ) -> Result<Vec<NamespaceIdent>> {
        let parent_ident = NamespaceIdent::new(parent.into());

        self.with_auth(|| self.inner.list_namespaces(Some(&parent_ident)))
            .await
            .err_into()
    }

    /// List all tables in a namespace.
    pub async fn list_tables(&self, namespace: impl Into<String>) -> Result<Vec<TableIdent>> {
        let namespace_ident = NamespaceIdent::new(namespace.into());

        self.with_auth(|| self.inner.list_tables(&namespace_ident))
            .await
            .err_into()
    }
}
