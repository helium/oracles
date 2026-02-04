use crate::{Error, Result, Settings};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;
use std::sync::Arc;

/// A wrapper around `RestCatalog` that is `Clone` and shareable across tasks.
///
/// Since `RestCatalog` from the iceberg crate is not `Clone`, this struct wraps it
/// in an `Arc` to enable sharing a single catalog connection across multiple
/// components like `IcebergTable` and `TableCreator`.
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<RestCatalog>,
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

        Ok(Self {
            inner: Arc::new(catalog),
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
}

impl AsRef<RestCatalog> for Catalog {
    fn as_ref(&self) -> &RestCatalog {
        &self.inner
    }
}
