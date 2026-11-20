//! Create the mobile-verifier tables that dbt models read, from their real
//! `table_definition()`s, in a local Iceberg catalog.
//!
//! dbt's CI (and `dbt/scripts/local_up.sh`) runs this against the repo's
//! docker-compose stack before building the models, so a source declaration in
//! `dbt/models/sources.yml` is checked against the schema the oracle actually
//! writes rather than a hand-maintained copy of it. A column renamed here fails
//! the dbt build in the same pull request.
//!
//! Only tables a dbt model reads belong in [`SOURCES`]. Idempotent: an existing
//! table is left alone.
//!
//! ```text
//! cargo run -p mobile-verifier --example create_dbt_sources
//! ```
//!
//! Connection defaults match docker-compose.yml from the host; override with
//! `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_CREDENTIAL`,
//! `ICEBERG_SCOPE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and
//! `S3_REGION`.

use helium_iceberg::{AuthConfig, S3Config, Settings, TableDefinition};
use mobile_verifier::iceberg::{heartbeat_report, speedtest, NAMESPACE};

/// The tables dbt reads, by definition function.
const SOURCES: &[fn() -> helium_iceberg::Result<TableDefinition>] = &[
    speedtest::table_definition,
    heartbeat_report::table_definition,
];

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let warehouse = env_or("ICEBERG_WAREHOUSE", "iceberg");
    let settings = Settings {
        catalog_uri: env_or("ICEBERG_CATALOG_URI", "http://localhost:8181/api/catalog"),
        catalog_name: warehouse.clone(),
        warehouse: Some(warehouse),
        auth: AuthConfig {
            credential: Some(env_or("ICEBERG_CREDENTIAL", "root:s3cr3t")),
            scope: Some(env_or("ICEBERG_SCOPE", "PRINCIPAL_ROLE:ALL")),
            ..Default::default()
        },
        s3: S3Config {
            endpoint: Some(env_or("S3_ENDPOINT", "http://localhost:9000")),
            access_key_id: Some(env_or("S3_ACCESS_KEY", "admin")),
            secret_access_key: Some(env_or("S3_SECRET_KEY", "admin")),
            region: Some(env_or("S3_REGION", "us-east-1")),
            path_style_access: Some(true),
            disable_config_load: Some(true),
        },
        properties: Default::default(),
    };

    let catalog = settings.connect().await?;
    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    for definition in SOURCES {
        let definition = definition()?;
        let name = definition.name().to_string();
        catalog.create_table_if_not_exists::<()>(definition).await?;
        println!("ensured {NAMESPACE}.{name}");
    }

    Ok(())
}
