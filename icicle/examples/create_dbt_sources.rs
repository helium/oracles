//! Create the icicle tables that dbt models read, from their real
//! [`GlaconOutput::table`] definitions, in a local Iceberg catalog.
//!
//! dbt's CI runs this against the repo's docker-compose stack before building
//! the models, so `chain.mobile_hotspot_history` in `dbt/models/sources.yml` is
//! checked against the schema icicle actually writes rather than a
//! hand-maintained copy of it. A column renamed here fails the dbt build in the
//! same pull request. Idempotent: an existing table is left alone.
//!
//! ```text
//! cargo run -p icicle --example create_dbt_sources
//! ```
//!
//! Connection defaults match docker-compose.yml from the host; override with
//! `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_CREDENTIAL`,
//! `ICEBERG_SCOPE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and
//! `S3_REGION`.

use glacon_helium::GlaconOutput;
use helium_iceberg::{AuthConfig, S3Config, Settings};
use icicle::data::mobile_hotspot_history::{
    MobileHotspotHistoryTable, DEFAULT_NAMESPACE as NAMESPACE,
};

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

    let definition = MobileHotspotHistoryTable::default().table()?;
    let name = definition.name().to_string();
    catalog.create_table_if_not_exists::<()>(definition).await?;
    println!("ensured {NAMESPACE}.{name}");

    Ok(())
}
