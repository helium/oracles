//! Iceberg REST catalog client with automatic retry on 401 auth errors.
//!
//! OAuth2 token management is delegated to the underlying `RestCatalog` from
//! iceberg-rust (configured via the `config` map passed to
//! `RestCatalogBuilder::load`). The wrapper here adds two things:
//!
//! - `Clone` — wraps `RestCatalog` (non-`Clone`) in an `Arc` so the catalog
//!   can be shared across `IcebergTable` / `TableCreator` / callers.
//! - 401 retry — if an operation fails with an auth error, invalidate the
//!   cached token and retry once.

use crate::error::IntoHeliumIcebergError;
use crate::{Error, IcebergTable, Result, Settings, TableCreator, TableDefinition};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
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
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<RestCatalog>,
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
                tracing::warn!("auth error, invalidating token and retrying");
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

        Ok(Self {
            inner: Arc::new(rest_catalog),
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
