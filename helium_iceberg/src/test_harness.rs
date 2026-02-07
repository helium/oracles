//! Test harness for Iceberg integration tests.
//!
//! Provides isolated schema environments for each test with a pre-configured Trino client.
//! Each test gets a unique schema within a shared catalog for isolation.
//!
//! # Example
//!
//! ```ignore
//! #[tokio::test]
//! async fn test_query_people() -> Result<()> {
//!     let harness = IcebergTestHarness::new().await?;
//!
//!     harness.create_table(
//!         TableDefinition::builder("people")
//!             .with_fields([
//!                 FieldDefinition::required("name", PrimitiveType::String),
//!                 FieldDefinition::required("age", PrimitiveType::Int),
//!             ])
//!             .build()?
//!     ).await?;
//!
//!     // Trino client is pre-configured with catalog.schema
//!     // Just reference table names directly
//!     let results = harness.trino().execute("SELECT * FROM people").await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;

use iceberg::{Catalog as IcebergCatalog, NamespaceIdent};
use trino_rust_client::ClientBuilder;
use uuid::Uuid;

use crate::settings::Settings;
use crate::{Catalog, Error, Result, TableCreator, TableDefinition};

/// Default Trino server host.
const DEFAULT_TRINO_HOST: &str = "localhost";

/// Default Trino server port.
const DEFAULT_TRINO_PORT: u16 = 8080;

/// Default catalog name (shared across tests).
const DEFAULT_CATALOG_NAME: &str = "iceberg";

/// Default Iceberg REST catalog URL (from host, for test harness).
const DEFAULT_ICEBERG_REST_URL: &str = "http://localhost:9001/iceberg";

/// Test harness providing isolated Iceberg schema environments.
///
/// Each instance creates a unique schema within a shared catalog.
/// The Trino client is pre-configured with the catalog and schema,
/// so queries can reference tables directly without qualification.
pub struct IcebergTestHarness {
    schema_name: String,
    trino: trino_rust_client::Client,
    iceberg_catalog: Catalog,
}

impl IcebergTestHarness {
    /// Create a new test harness with default configuration.
    ///
    /// This will:
    /// 1. Connect to the shared catalog
    /// 2. Create a unique schema `test_{uuid}`
    /// 3. Configure Trino client with the catalog and schema
    pub async fn new() -> Result<Self> {
        Self::with_config(HarnessConfig::default()).await
    }

    /// Create a new test harness with custom configuration.
    pub async fn with_config(config: HarnessConfig) -> Result<Self> {
        let schema_name = format!("test_{}", Uuid::new_v4().as_simple());

        // Connect to Iceberg catalog
        let iceberg_settings = Settings {
            catalog_uri: config.iceberg_rest_url.clone(),
            catalog_name: config.catalog_name.clone(),
            warehouse: None,
            auth_token: None,
            s3_access_key: Some(config.s3_access_key.clone()),
            s3_secret_key: Some(config.s3_secret_key.clone()),
        };
        let iceberg_catalog = Catalog::connect(&iceberg_settings).await?;

        // Create unique namespace for this test
        let ns = NamespaceIdent::new(namespace.clone());
        iceberg_catalog
            .as_ref()
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(Error::Iceberg)?;

        // Create Trino client with catalog and schema pre-configured
        let trino = ClientBuilder::new(&config.trino_user, &config.trino_host)
            .port(config.trino_port)
            .catalog(&config.catalog_name)
            .schema(&schema_name)
            .build()
            .map_err(|e| Error::Catalog(format!("failed to create trino client: {}", e)))?;

        tracing::info!(schema_name, catalog_name = %config.catalog_name, "test harness initialized");

        Ok(Self {
            schema_name,
            trino,
            iceberg_catalog,
        })
    }

    /// Get the Trino client for executing queries.
    ///
    /// The client is pre-configured with the test's catalog and schema,
    /// so you can reference tables directly (e.g., `SELECT * FROM my_table`).
    pub fn trino(&self) -> &trino_rust_client::Client {
        &self.trino
    }

    /// Get the Iceberg catalog for direct catalog operations.
    pub fn iceberg_catalog(&self) -> &Catalog {
        &self.iceberg_catalog
    }

    /// Get the unique schema name for this test (e.g., `test_abc123`).
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Create a table in the test schema using helium_iceberg.
    pub async fn create_table(&self, definition: TableDefinition) -> Result<()> {
        let creator = TableCreator::new(self.iceberg_catalog.clone());
        creator
            .create_table_if_not_exists(&self.schema_name, definition)
            .await?;
        Ok(())
    }
}

/// Configuration for the test harness.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub catalog_name: String,
    pub trino_host: String,
    pub trino_port: u16,
    pub trino_user: String,
    /// Iceberg REST URL from host (for test harness to connect).
    pub iceberg_rest_url: String,
    /// S3 access key for MinIO/S3 writes from the host.
    pub s3_access_key: String,
    /// S3 secret key for MinIO/S3 writes from the host.
    pub s3_secret_key: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            trino_host: DEFAULT_TRINO_HOST.to_string(),
            trino_port: DEFAULT_TRINO_PORT,
            trino_user: "test".to_string(),
            iceberg_rest_url: DEFAULT_ICEBERG_REST_URL.to_string(),
            s3_access_key: "admin".to_string(),
            s3_secret_key: "password".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldDefinition, PartitionDefinition, PrimitiveType, Type};

    #[tokio::test]
    async fn test_harness_basic() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new().await?;
        println!("Harness created with schema: {}", harness.schema_name());

        // Create a simple table
        harness
            .create_table(
                TableDefinition::builder("people")
                    .with_fields([
                        FieldDefinition::required("name", Type::Primitive(PrimitiveType::String)),
                        FieldDefinition::required("age", Type::Primitive(PrimitiveType::Int)),
                    ])
                    .with_location(format!("s3://iceberg/{}/people", harness.schema_name()))
                    .build()?,
            )
            .await?;
        println!("Table created");

        // Query via Trino - no need to qualify table name
        let _result = harness
            .trino()
            .execute("SELECT * FROM people".to_string())
            .await;
        println!("Query completed");

        Ok(())
    }

    #[tokio::test]
    async fn test_write_and_query() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new().await?;

        harness
            .create_table(
                TableDefinition::builder("events")
                    .with_fields([
                        FieldDefinition::required("id", Type::Primitive(PrimitiveType::String)),
                        FieldDefinition::required(
                            "timestamp",
                            Type::Primitive(PrimitiveType::Timestamptz),
                        ),
                    ])
                    .with_partition(PartitionDefinition::day("timestamp", "day"))
                    .with_location(format!("s3://iceberg/{}/events", harness.schema_name()))
                    .build()?,
            )
            .await?;

        // Note: Direct IcebergTable writes from host don't work because the REST catalog
        // commit uses its own FileIO with Docker-internal URLs (minio:9000).
        // Use Trino for inserts instead, or add "127.0.0.1 minio" to /etc/hosts.

        // Insert via Trino (works because Trino runs inside Docker)
        harness
            .trino()
            .execute(
                "INSERT INTO events (id, timestamp) VALUES ('event_1', CURRENT_TIMESTAMP)"
                    .to_string(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("insert failed: {}", e))?;

        // Query the data
        harness
            .trino()
            .execute("SELECT * FROM events".to_string())
            .await
            .map_err(|e| anyhow::anyhow!("query failed: {}", e))?;

        Ok(())
    }
}
