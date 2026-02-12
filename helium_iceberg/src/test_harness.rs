//! Test harness for Iceberg integration tests.
//!
//! Provides isolated catalog environments for each test with a pre-configured Trino client.
//! Each test gets its own Polaris catalog for full isolation, registered with Trino via
//! `CREATE CATALOG`.
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

use futures::TryFutureExt;
use serde::Serialize;
use trino_rust_client::ClientBuilder;
use uuid::Uuid;

use crate::settings::{AuthConfig, S3Config, Settings};
use crate::writer::IntoBoxedDataWriter;
use crate::{BoxedDataWriter, Catalog, Error, IcebergTable, Result, TableCreator, TableDefinition};

/// Default Trino server host.
const DEFAULT_TRINO_HOST: &str = "localhost";

/// Default Trino server port.
const DEFAULT_TRINO_PORT: u16 = 8080;

/// Default Iceberg REST catalog URL (from host, for test harness).
const DEFAULT_ICEBERG_REST_URL: &str = "http://localhost:8181/api/catalog";

/// Polaris Management API URL (from host).
const DEFAULT_POLARIS_MANAGEMENT_URL: &str = "http://localhost:8181/api/management/v1";

/// Polaris OAuth2 token URL (from host).
const DEFAULT_POLARIS_TOKEN_URL: &str = "http://localhost:8181/api/catalog/v1/oauth/tokens";

/// Iceberg REST URL as seen from inside Docker (Trino → Polaris).
const DEFAULT_DOCKER_ICEBERG_REST_URL: &str = "http://polaris:8181/api/catalog";

/// S3/MinIO endpoint as seen from inside Docker (Trino → MinIO).
const DEFAULT_DOCKER_S3_ENDPOINT: &str = "http://minio:9000";

/// Default namespace within each catalog.
const DEFAULT_NAMESPACE: &str = "default";

#[derive(Debug, thiserror::Error)]
enum TestHarnessError {
    #[error("{description} request failed: {source:?}")]
    PolarisRequest {
        description: &'static str,
        source: reqwest::Error,
    },

    #[error("{description} returned: {source:?}")]
    PolarisResponse {
        description: &'static str,
        source: reqwest::Error,
    },

    #[error("failed to parse OAuth2 token: {0}")]
    PolarisTokenParse(reqwest::Error),

    #[error("failed to create trino client: {0}")]
    TrinoClientBuild(trino_rust_client::error::Error),

    #[error("failed to register catalog with Trino: {0}")]
    TrinoRegisterCatalog(trino_rust_client::error::Error),
}

/// Test harness providing isolated Iceberg catalog environments.
///
/// Each instance creates its own Polaris catalog registered with Trino.
/// The Trino client is pre-configured with the catalog and schema,
/// so queries can reference tables directly without qualification.
pub struct IcebergTestHarness {
    catalog_name: String,
    namespace: String,
    trino: trino_rust_client::Client,
    iceberg_catalog: Catalog,
}

impl IcebergTestHarness {
    /// Create a new test harness with default configuration.
    ///
    /// This will:
    /// 1. Create a unique Polaris catalog `test_{uuid}`
    /// 2. Register it with Trino via `CREATE CATALOG`
    /// 3. Create a `default` namespace
    /// 4. Configure Trino client with the catalog and schema
    pub async fn new() -> Result<Self> {
        Self::with_config(HarnessConfig::default()).await
    }

    pub async fn new_with_tables(
        tables: impl IntoIterator<Item = TableDefinition>,
    ) -> Result<Self> {
        let harness = Self::new().await?;
        for table in tables {
            harness.create_table(table).await?;
        }
        Ok(harness)
    }

    /// Create a new test harness with custom configuration.
    pub async fn with_config(config: HarnessConfig) -> Result<Self> {
        let catalog_name = format!("test_{}", Uuid::new_v4().as_simple());
        let namespace = DEFAULT_NAMESPACE.to_string();

        let http_client = reqwest::Client::new();

        let token = fetch_polaris_token(
            &http_client,
            &config.polaris_token_url,
            &config.oauth2_credential,
            &config.oauth2_scope,
        )
        .await?;

        create_polaris_catalog(
            &http_client,
            &config.polaris_management_url,
            &token,
            &catalog_name,
        )
        .await?;

        register_trino_catalog(&config, &catalog_name).await?;

        let iceberg_catalog =
            connect_to_iceberg_catalog_with_namespace(&config, &catalog_name, &namespace).await?;

        let trino = create_trino_client(&config, &catalog_name, &namespace).await?;

        tracing::info!(
            %catalog_name,
            %namespace,
            "test harness initialized with dedicated catalog"
        );

        Ok(Self {
            catalog_name,
            namespace,
            trino,
            iceberg_catalog,
        })
    }

    pub async fn get_table_writer<T>(
        &self,
        table_name: impl Into<String>,
    ) -> Result<BoxedDataWriter<T>>
    where
        T: Serialize + Sync + Send + 'static,
    {
        IcebergTable::from_catalog(self.iceberg_catalog.clone(), self.namespace(), table_name)
            .await
            .map(|writer| writer.boxed())
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

    /// Get the unique catalog name for this test (e.g., `test_abc123`).
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Get the namespace name for this test (always `default`).
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Create a table in the test namespace using helium_iceberg.
    pub async fn create_table(&self, definition: TableDefinition) -> Result<()> {
        let creator = TableCreator::new(self.iceberg_catalog.clone());
        creator
            .create_table_if_not_exists::<()>(&self.namespace, definition)
            .await?;
        Ok(())
    }
}

/// Configuration for the test harness.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub trino_host: String,
    pub trino_port: u16,
    pub trino_user: String,
    /// Iceberg REST URL from host (for test harness to connect).
    pub iceberg_rest_url: String,
    /// Polaris Management API URL (from host).
    pub polaris_management_url: String,
    /// Polaris OAuth2 token URL (from host).
    pub polaris_token_url: String,
    /// Iceberg REST URL as seen from Docker (for Trino config).
    pub docker_iceberg_rest_url: String,
    /// S3/MinIO endpoint as seen from Docker (for Trino config).
    pub docker_s3_endpoint: String,
    /// OAuth2 credential (client_id:client_secret) for Polaris.
    pub oauth2_credential: String,
    /// OAuth2 scope for Polaris.
    pub oauth2_scope: String,
    /// S3/MinIO endpoint URL from host.
    pub s3_endpoint: String,
    /// S3 access key for MinIO/S3 writes from the host.
    pub s3_access_key: String,
    /// S3 secret key for MinIO/S3 writes from the host.
    pub s3_secret_key: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            trino_host: DEFAULT_TRINO_HOST.to_string(),
            trino_port: DEFAULT_TRINO_PORT,
            trino_user: "test".to_string(),
            iceberg_rest_url: DEFAULT_ICEBERG_REST_URL.to_string(),
            polaris_management_url: DEFAULT_POLARIS_MANAGEMENT_URL.to_string(),
            polaris_token_url: DEFAULT_POLARIS_TOKEN_URL.to_string(),
            docker_iceberg_rest_url: DEFAULT_DOCKER_ICEBERG_REST_URL.to_string(),
            docker_s3_endpoint: DEFAULT_DOCKER_S3_ENDPOINT.to_string(),
            oauth2_credential: "root:s3cr3t".to_string(),
            oauth2_scope: "PRINCIPAL_ROLE:ALL".to_string(),
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_access_key: "admin".to_string(),
            s3_secret_key: "password".to_string(),
        }
    }
}

/// Fetch an OAuth2 bearer token from the Polaris token endpoint.
async fn fetch_polaris_token(
    client: &reqwest::Client,
    token_url: &str,
    credential: &str,
    scope: &str,
) -> Result<String> {
    let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

    let response = client
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", scope),
        ])
        .send()
        .await
        .map_test_harness_err("OAuth 2 token")?;

    /// OAuth2 token response.
    #[derive(serde::Deserialize)]
    struct TokenResponse {
        access_token: String,
    }

    let token = response
        .json::<TokenResponse>()
        .map_err(TestHarnessError::PolarisTokenParse)
        .await?;

    Ok(token.access_token)
}

/// Create a new Polaris catalog with full admin permissions.
///
/// Mirrors the `polaris-setup` service from `iceberg-compose.yml`:
/// 1. Create catalog with S3 storage config
/// 2. Create `admin` catalog role
/// 3. Grant `CATALOG_MANAGE_CONTENT` to admin role
/// 4. Assign admin role to `service_admin` principal role
async fn create_polaris_catalog(
    client: &reqwest::Client,
    management_url: &str,
    token: &str,
    catalog_name: &str,
) -> Result<()> {
    let auth_header = format!("Bearer {token}");

    // 1. Create catalog
    let payload = serde_json::json!({
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": false,
            "properties": {
                "default-base-location": format!("s3://iceberg/{catalog_name}")
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [format!("s3://iceberg/{catalog_name}")],
                "endpoint": "http://localhost:9000",
                "endpointInternal": "http://minio:9000",
                "pathStyleAccess": true
            }
        }
    });

    client
        .post(format!("{management_url}/catalogs"))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
        .map_test_harness_err("create catalog")?;

    // 2. Create admin catalog role
    client
        .post(format!(
            "{management_url}/catalogs/{catalog_name}/catalog-roles"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"catalogRole": {"name": "admin"}}))
        .send()
        .await
        .map_test_harness_err("create catalog role")?;

    // 3. Grant CATALOG_MANAGE_CONTENT to admin role
    client
        .put(format!(
            "{management_url}/catalogs/{catalog_name}/catalog-roles/admin/grants"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}))
        .send()
        .await.map_test_harness_err("grant catalog privilege")?;

    // 4. Assign admin role to service_admin principal role
    client
        .put(format!(
            "{management_url}/principal-roles/service_admin/catalog-roles/{catalog_name}"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"catalogRole": {"name": "admin"}}))
        .send()
        .await
        .map_test_harness_err("assign catalog role")?;

    Ok(())
}

/// Register a Polaris catalog with Trino via `CREATE CATALOG` SQL.
///
/// Uses Docker-internal URLs since Trino runs inside Docker.
async fn register_trino_catalog(config: &HarnessConfig, catalog_name: &str) -> Result<()> {
    // Build a temporary Trino client with no catalog/schema set
    let trino = ClientBuilder::new(&config.trino_user, &config.trino_host)
        .port(config.trino_port)
        .build()
        .map_err(TestHarnessError::TrinoClientBuild)?;

    let sql = format!(
        r#"
        CREATE CATALOG "{catalog_name}" USING iceberg WITH (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = '{docker_rest_url}',
            "iceberg.rest-catalog.security" = 'OAUTH2',
            "iceberg.rest-catalog.oauth2.credential" = '{credential}',
            "iceberg.rest-catalog.oauth2.scope" = '{scope}',
            "iceberg.rest-catalog.warehouse" = '{catalog_name}',
            "iceberg.file-format" = 'parquet',
            "fs.native-s3.enabled" = 'true',
            "s3.endpoint" = '{docker_s3}',
            "s3.region" = 'us-east-1',
            "s3.aws-access-key" = '{s3_key}',
            "s3.aws-secret-key" = '{s3_secret}',
            "s3.path-style-access" = 'true'
        )
        "#,
        docker_rest_url = config.docker_iceberg_rest_url,
        credential = config.oauth2_credential,
        scope = config.oauth2_scope,
        docker_s3 = config.docker_s3_endpoint,
        s3_key = config.s3_access_key,
        s3_secret = config.s3_secret_key,
    );

    trino
        .execute(sql)
        .map_err(TestHarnessError::TrinoRegisterCatalog)
        .await?;

    Ok(())
}

async fn connect_to_iceberg_catalog_with_namespace(
    config: &HarnessConfig,
    catalog_name: &str,
    namespace: &str,
) -> Result<Catalog> {
    let iceberg_settings = Settings {
        catalog_uri: config.iceberg_rest_url.clone(),
        catalog_name: catalog_name.to_string(),
        warehouse: Some(catalog_name.to_string()),
        auth: AuthConfig {
            credential: Some(config.oauth2_credential.clone()),
            scope: Some(config.oauth2_scope.clone()),
            ..Default::default()
        },
        s3: S3Config {
            endpoint: Some(config.s3_endpoint.clone()),
            access_key_id: Some(config.s3_access_key.clone()),
            secret_access_key: Some(config.s3_secret_key.clone()),
            region: Some("us-east-1".to_string()),
            path_style_access: Some(true),
        },
        properties: Default::default(),
    };

    let catalog = Catalog::connect(&iceberg_settings).await?;
    catalog.create_namespace_if_not_exists(namespace).await?;

    Ok(catalog)
}

async fn create_trino_client(
    config: &HarnessConfig,
    catalog_name: &str,
    namespace: &str,
) -> Result<trino_rust_client::Client> {
    ClientBuilder::new(&config.trino_user, &config.trino_host)
        .port(config.trino_port)
        .catalog(catalog_name)
        .schema(namespace)
        .build()
        .map_err(|e| Error::Catalog(format!("failed to create trino client: {e:?}")))
}

// Helper for mapping polaris reqeuest/response errors separately.
trait TestHarnessErrorExt {
    fn map_test_harness_err(self, description: &'static str) -> Result<reqwest::Response>;
}

impl TestHarnessErrorExt for reqwest::Result<reqwest::Response> {
    fn map_test_harness_err(self, description: &'static str) -> Result<reqwest::Response> {
        let resp = self
            .map_err(|source| TestHarnessError::PolarisRequest {
                description,
                source,
            })?
            .error_for_status()
            .map_err(|source| TestHarnessError::PolarisResponse {
                description,
                source,
            })?;

        Ok(resp)
    }
}

impl From<TestHarnessError> for Error {
    fn from(error: TestHarnessError) -> Self {
        Error::Catalog(format!("{error}"))
    }
}

#[cfg(test)]
mod tests {

    use super::{IcebergTestHarness, TableDefinition};
    use crate::{FieldDefinition, PartitionDefinition, PrimitiveType};

    use chrono::{DateTime, FixedOffset, Utc};
    use serde::{Deserialize, Serialize};
    use trino_rust_client::Trino;

    #[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
    struct Person {
        name: String,
        age: u32,
        inserted: DateTime<FixedOffset>,
    }

    fn person_table_def() -> anyhow::Result<TableDefinition> {
        let def = TableDefinition::builder("people")
            .with_fields([
                FieldDefinition::required("name", PrimitiveType::String),
                FieldDefinition::required("age", PrimitiveType::Int),
                FieldDefinition::required("inserted", PrimitiveType::Timestamptz),
            ])
            .with_partition(PartitionDefinition::day("inserted", "inserted_day"))
            .build()?;

        Ok(def)
    }

    #[tokio::test]
    #[ignore = "need polaris/trino/minio running in env"]
    async fn make_test_harness_catalog_per_test() -> anyhow::Result<()> {
        let harness_1 = IcebergTestHarness::new().await?;
        let harness_2 = IcebergTestHarness::new().await?;

        assert_ne!(harness_1.catalog_name(), harness_2.catalog_name());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need polaris/trino/minio running in env"]
    async fn test_query_table() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let _res = harness
            .trino()
            .execute("SELECT * FROM people".to_string())
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need polaris/trino/minio running in env"]
    async fn test_missing_table_query_fails() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let res = harness
            .trino()
            .execute("SELECT * FROM bad_table".to_string())
            .await;

        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need polaris/trino/minio running in env"]
    async fn test_missing_field_query_fails() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let res = harness
            .trino()
            .execute("SELECT no_field FROM people".to_string())
            .await;

        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need polaris/trino/minio running in env"]
    async fn test_write_and_query() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let people = vec![
            Person {
                name: "Alice".to_string(),
                age: 42,
                inserted: Utc::now().into(),
            },
            Person {
                name: "Bob".to_string(),
                age: 42,
                inserted: Utc::now().into(),
            },
        ];

        let writer = harness.get_table_writer("people").await?;
        writer.write(people.clone()).await?;

        let queried_people = harness
            .trino()
            .get_all::<Person>("SELECT * FROM people".to_string())
            .await?
            .into_vec();
        assert_eq!(queried_people, people);

        Ok(())
    }
}
