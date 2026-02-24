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

use std::collections::HashMap;

use derive_builder::Builder;
use futures::TryFutureExt;
use serde::Serialize;
use tokio::sync::Mutex;
use trino_rust_client::ClientBuilder;
use uuid::Uuid;

use crate::settings::{AuthConfig, S3Config, Settings};
use crate::writer::IntoBoxedDataWriter;
use crate::{BoxedDataWriter, Catalog, Error, IcebergTable, Result, TableCreator, TableDefinition};

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
    trino: trino_rust_client::Client,
    iceberg_catalog: Catalog,
    table_namespaces: Mutex<HashMap<String, String>>,
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
            &config.catalog_local_url(),
            &config.catalog_oauth2_credential,
            &config.catalog_oauth2_scope,
        )
        .await?;

        create_polaris_catalog(&http_client, &config, &token, &catalog_name).await?;

        register_trino_catalog(&config, &catalog_name).await?;

        let iceberg_catalog =
            connect_to_iceberg_catalog_with_namespace(&config, &catalog_name, &namespace).await?;

        let trino = create_trino_client(&config, &catalog_name).await?;

        tracing::info!(
            %catalog_name,
            %namespace,
            "test harness initialized with dedicated catalog"
        );

        Ok(Self {
            catalog_name,
            trino,
            iceberg_catalog,
            table_namespaces: Mutex::new(HashMap::default()),
        })
    }

    pub async fn get_table_writer<T>(
        &self,
        table_name: impl Into<String>,
    ) -> Result<BoxedDataWriter<T>>
    where
        T: Serialize + Sync + Send + 'static,
    {
        let table_name = table_name.into();
        let namespace = self
            .table_namespaces
            .lock()
            .await
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| "unknown_namespace".to_string());

        self.get_table_writer_in(namespace, table_name).await
    }

    /// Get a data writer for a table in a specific namespace.
    pub async fn get_table_writer_in<T>(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<BoxedDataWriter<T>>
    where
        T: Serialize + Sync + Send + 'static,
    {
        let namespace = namespace.into();
        let table_name = table_name.into();

        if !self.table_exists_in(&namespace, &table_name).await? {
            return Err(Error::TableNotFound {
                namespace,
                table: table_name,
            });
        }

        IcebergTable::from_catalog(self.iceberg_catalog.clone(), &namespace, table_name)
            .await
            .map(|writer| writer.boxed())
    }

    async fn table_exists_in(&self, namespace: &str, table_name: &str) -> Result<bool> {
        self.iceberg_catalog
            .table_exists(namespace, table_name)
            .await
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

    /// Create a table in the test catalog using helium_iceberg.
    pub async fn create_table(&self, definition: TableDefinition) -> Result<()> {
        self.iceberg_catalog
            .create_namespace_if_not_exists(definition.namespace())
            .await?;

        self.table_namespaces.lock().await.insert(
            definition.name().to_string(),
            definition.namespace().to_string(),
        );

        let creator = TableCreator::new(self.iceberg_catalog.clone());
        creator.create_table_if_not_exists::<()>(definition).await?;
        Ok(())
    }
}

/// Configuration for the test harness.
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(pattern = "owned")]
pub struct HarnessConfig {
    /// S3 host name when talking to s3 with a mounted port
    #[builder(default = "env_defaults::s3_local()")]
    pub s3_host_local: TestHost,
    /// S3 host name when talking to s3 from inside another container
    #[builder(default = "env_defaults::s3_qualified()")]
    pub s3_host_qualified: TestHost,
    /// S3 access key for MinIO/S3 writes from the host.
    #[builder(default = "env_defaults::s3_access_key()")]
    pub s3_access_key: String,
    /// S3 secret key for MinIO/S3 writes from the host.
    #[builder(default = "env_defaults::s3_secret_key()")]
    pub s3_secret_key: String,
    /// S3 region for MinIO/S3
    #[builder(default = "env_defaults::s3_region()")]
    pub s3_region: String,

    /// Catalog host name when talking to catalog with a mounted port
    #[builder(default = "env_defaults::catalog_local()")]
    pub catalog_host_local: TestHost,
    /// Catalog host name when talking to catalog from inside another conatiner
    #[builder(default = "env_defaults::catalog_qualified()")]
    pub catalog_host_qualified: TestHost,
    /// OAuth2 credential (client_id:client_secret) for Polaris.
    #[builder(default = "env_defaults::catalog_oauth2_credential()")]
    pub catalog_oauth2_credential: String,
    /// OAuth2 scope for Polaris.
    #[builder(default = "env_defaults::catalog_oauth2_scope()")]
    pub catalog_oauth2_scope: String,

    /// Trino host (normally localhost)
    #[builder(default = "env_defaults::trino_host()")]
    pub trino_host: String,
    /// Trino port (normally 9000)
    #[builder(default = "env_defaults::trino_port()")]
    pub trino_port: u16,
    /// Trino Username
    #[builder(default = "env_defaults::trino_user()")]
    pub trino_user: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        use env_defaults::*;
        Self {
            s3_host_local: s3_local(),
            s3_host_qualified: s3_qualified(),
            s3_access_key: s3_access_key(),
            s3_secret_key: s3_secret_key(),
            s3_region: s3_region(),

            catalog_host_local: catalog_local(),
            catalog_host_qualified: catalog_qualified(),
            catalog_oauth2_credential: catalog_oauth2_credential(),
            catalog_oauth2_scope: catalog_oauth2_scope(),

            trino_host: trino_host(),
            trino_port: trino_port(),
            trino_user: trino_user(),
        }
    }
}

impl HarnessConfig {
    pub fn builder() -> HarnessConfigBuilder {
        HarnessConfigBuilder::default()
    }

    fn s3_local_url(&self) -> String {
        self.s3_host_local.with_path("")
    }

    fn s3_qualified_url(&self) -> String {
        self.s3_host_qualified.with_path("")
    }

    fn catalog_management_local_url(&self) -> String {
        self.catalog_host_local.with_path("/api/management/v1")
    }

    fn catalog_local_url(&self) -> String {
        self.catalog_host_local.with_path("/api/catalog")
    }

    fn catalog_qualified_url(&self) -> String {
        self.catalog_host_qualified.with_path("/api/catalog")
    }

    fn trino_client_builder(&self) -> ClientBuilder {
        ClientBuilder::new(&self.trino_user, &self.trino_host).port(self.trino_port)
    }
}

/// Fetch an OAuth2 bearer token from the Polaris token endpoint.
async fn fetch_polaris_token(
    client: &reqwest::Client,
    catalog_url: &str,
    credential: &str,
    scope: &str,
) -> Result<String> {
    let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

    let token_url = format!("{catalog_url}/v1/oauth/tokens");
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
    config: &HarnessConfig,
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
                "default-base-location": format!("s3://iceberg-test/{catalog_name}")
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [format!("s3://iceberg-test/{catalog_name}")],
                "endpoint": config.s3_local_url(),
                "endpointInternal": config.s3_qualified_url(),
                "pathStyleAccess": true
            }
        }
    });

    let management_url = config.catalog_management_local_url();

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
    let trino = config
        .trino_client_builder()
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
            "s3.region" = '{s3_region}',
            "s3.aws-access-key" = '{s3_key}',
            "s3.aws-secret-key" = '{s3_secret}',
            "s3.path-style-access" = 'true'
        )
        "#,
        docker_rest_url = config.catalog_qualified_url(),
        credential = config.catalog_oauth2_credential,
        scope = config.catalog_oauth2_scope,
        docker_s3 = config.s3_qualified_url(),
        s3_key = config.s3_access_key,
        s3_secret = config.s3_secret_key,
        s3_region = config.s3_region,
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
        catalog_uri: config.catalog_local_url(),
        catalog_name: catalog_name.to_string(),
        warehouse: Some(catalog_name.to_string()),
        auth: AuthConfig {
            credential: Some(config.catalog_oauth2_credential.clone()),
            scope: Some(config.catalog_oauth2_scope.clone()),
            ..Default::default()
        },
        s3: S3Config {
            endpoint: Some(config.s3_qualified_url()),
            access_key_id: Some(config.s3_access_key.clone()),
            secret_access_key: Some(config.s3_secret_key.clone()),
            region: Some(config.s3_region.clone()),
            path_style_access: Some(true),
        },
        properties: Default::default(),
    };

    let catalog = Catalog::connect(&iceberg_settings).await?;
    catalog.create_namespace_if_not_exists(namespace).await?;

    Ok(catalog)
}

// NOTE: The namespace argument is purposefully left blank here. We've the
// decision that TableDefinition should declare what namespace a table is in,
// and queries to that table should contain the namespace.
async fn create_trino_client(
    config: &HarnessConfig,
    catalog_name: &str,
) -> Result<trino_rust_client::Client> {
    config
        .trino_client_builder()
        .catalog(catalog_name)
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

#[derive(Debug, Clone, PartialEq)]
pub struct TestHost {
    pub host: String,
    pub port: u16,
}

impl TestHost {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    fn with_path(&self, path: &str) -> String {
        format!("http://{}:{}{path}", self.host, self.port)
    }
}

mod env_defaults {
    use crate::test_harness::TestHost;

    const S3_LOCAL_HOST_ENV: &str = "S3_LOCAL_HOST";
    const S3_LOCAL_PORT_ENV: &str = "S3_LOCAL_PORT";
    const S3_QUALIFIED_HOST_ENV: &str = "S3_QUALIFIED_HOST";
    const S3_QUALIFIED_PORT_ENV: &str = "S3_QUALIFIED_PORT";
    const S3_ACCESS_KEY_ENV: &str = "S3_ACCESS";
    const S3_SECRET_KEY_ENV: &str = "S3_SECRET";
    const S3_REGION_ENV: &str = "S3_REGION";
    const S3_DEFAULT_PORT: u16 = 9000;

    const CATALOG_LOCAL_HOST_ENV: &str = "CATALOG_LOCAL_HOST";
    const CATALOG_LOCAL_PORT_ENV: &str = "CATALOG_LOCAL_PORT";
    const CATALOG_QUALIFIED_HOST_ENV: &str = "CATALOG_QUALIFIED_HOST";
    const CATALOG_QUALIFIED_PORT_ENV: &str = "CATALOG_QUALIFIED_PORT";
    const CATALOG_OAUTH2_CREDENTIAL_ENV: &str = "CATALOG_OAUTH2_CREDENTIAL";
    const CATALOG_OAUTH2_SCOPE_ENV: &str = "CATALOG_OAUTH2_SCOPE";
    const CATALOG_DEFAULT_PORT: u16 = 8181;

    const TRINO_HOST_ENV: &str = "TRINO_HOST";
    const TRINO_PORT_ENV: &str = "TRINO_PORT";
    const TRINO_USER_ENV: &str = "TRINO_USER";
    const TRINO_DEFAULT_PORT: u16 = 8080;

    // ======= S3 ====================
    pub fn s3_local_host() -> String {
        env_str(S3_LOCAL_HOST_ENV, "localhost")
    }
    pub fn s3_local_port() -> u16 {
        env_port(S3_LOCAL_PORT_ENV, S3_DEFAULT_PORT)
    }
    pub fn s3_local() -> TestHost {
        TestHost::new(s3_local_host(), s3_local_port())
    }

    pub fn s3_qualified_host() -> String {
        env_str(S3_QUALIFIED_HOST_ENV, "minio")
    }
    pub fn s3_qualified_port() -> u16 {
        env_port(S3_QUALIFIED_PORT_ENV, S3_DEFAULT_PORT)
    }
    pub fn s3_qualified() -> TestHost {
        TestHost::new(s3_qualified_host(), s3_qualified_port())
    }

    pub fn s3_access_key() -> String {
        env_str(S3_ACCESS_KEY_ENV, "admin")
    }
    pub fn s3_secret_key() -> String {
        env_str(S3_SECRET_KEY_ENV, "password")
    }
    pub fn s3_region() -> String {
        env_str(S3_REGION_ENV, "us-east-1")
    }

    // ======= Catalog ====================
    pub fn catalog_local_host() -> String {
        env_str(CATALOG_LOCAL_HOST_ENV, "localhost")
    }
    pub fn catalog_local_port() -> u16 {
        env_port(CATALOG_LOCAL_PORT_ENV, CATALOG_DEFAULT_PORT)
    }
    pub fn catalog_local() -> TestHost {
        TestHost::new(catalog_local_host(), catalog_local_port())
    }

    pub fn catalog_qualified_host() -> String {
        env_str(CATALOG_QUALIFIED_HOST_ENV, "polaris")
    }
    pub fn catalog_qualified_port() -> u16 {
        env_port(CATALOG_QUALIFIED_PORT_ENV, CATALOG_DEFAULT_PORT)
    }
    pub fn catalog_qualified() -> TestHost {
        TestHost::new(catalog_qualified_host(), catalog_qualified_port())
    }

    pub fn catalog_oauth2_credential() -> String {
        env_str(CATALOG_OAUTH2_CREDENTIAL_ENV, "root:s3cr3t")
    }
    pub fn catalog_oauth2_scope() -> String {
        env_str(CATALOG_OAUTH2_SCOPE_ENV, "PRINCIPAL_ROLE:ALL")
    }

    // ======= Trino ====================
    pub fn trino_host() -> String {
        env_str(TRINO_HOST_ENV, "localhost")
    }
    pub fn trino_port() -> u16 {
        env_port(TRINO_PORT_ENV, TRINO_DEFAULT_PORT)
    }
    pub fn trino_user() -> String {
        env_str(TRINO_USER_ENV, "test")
    }

    // ======= Helpers ====================
    fn to_u16(port: String, label: &str) -> u16 {
        port.parse::<u16>()
            .unwrap_or_else(|val| panic!("u16 parseable {label} port: {val}"))
    }
    fn env_str(var: &str, default: &str) -> String {
        std::env::var(var).unwrap_or_else(|_| default.to_string())
    }
    fn env_port(var: &str, default: u16) -> u16 {
        std::env::var(var)
            .map(|port| to_u16(port, var))
            .unwrap_or(default)
    }
}

#[cfg(test)]
mod tests {

    use super::{IcebergTestHarness, TableDefinition};
    use crate::{FieldDefinition, PartitionDefinition};

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
        let def = TableDefinition::builder("default", "people")
            .with_fields([
                FieldDefinition::required_string("name"),
                FieldDefinition::required_int("age"),
                FieldDefinition::required_timestamptz("inserted"),
            ])
            .with_partition(PartitionDefinition::day("inserted", "inserted_day"))
            .build()?;

        Ok(def)
    }

    #[tokio::test]
    async fn make_test_harness_catalog_per_test() -> anyhow::Result<()> {
        let harness_1 = IcebergTestHarness::new().await?;
        let harness_2 = IcebergTestHarness::new().await?;

        assert_ne!(harness_1.catalog_name(), harness_2.catalog_name());

        Ok(())
    }

    #[tokio::test]
    async fn test_query_table() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let _res = harness
            .trino()
            .execute("SELECT * FROM default.people".to_string())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_missing_table_query_fails() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let res = harness
            .trino()
            .execute("SELECT * FROM default.bad_table".to_string())
            .await;

        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_missing_field_query_fails() -> anyhow::Result<()> {
        let harness = IcebergTestHarness::new_with_tables([person_table_def()?]).await?;

        let res = harness
            .trino()
            .execute("SELECT no_field FROM default.people".to_string())
            .await;

        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
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
            .get_all::<Person>("SELECT * FROM default.people".to_string())
            .await?
            .into_vec();
        assert_eq!(queried_people, people);

        Ok(())
    }
}
