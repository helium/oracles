use anyhow::Result;
use clap::Parser;
use helium_iceberg_oracles::rewards::{
    IcebergGatewayReward, IcebergServiceProviderReward, IcebergUnallocatedReward,
};
use reward_index::{iceberg, settings::Settings, telemetry};
use sqlx::PgPool;
use std::path::PathBuf;
use task_manager::TaskManager;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Indexer")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Server),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let app_name = format!("mobile_{}", env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        run(settings, pool).await
    }
}

/// Index rewards from the `rewards` Iceberg tables.
async fn run(settings: &Settings, pool: PgPool) -> Result<()> {
    let iceberg_settings = &settings.iceberg;
    let catalog = iceberg_settings.catalog.connect().await?;
    let unallocated_reward_key = &settings.unallocated_reward_entity_key;

    let (data_transfer_server, data_transfer_indexer) = iceberg::create::<IcebergGatewayReward>(
        catalog.clone(),
        iceberg_settings,
        pool.clone(),
        unallocated_reward_key.clone(),
    )
    .await?;
    let (service_provider_server, service_provider_indexer) =
        iceberg::create::<IcebergServiceProviderReward>(
            catalog.clone(),
            iceberg_settings,
            pool.clone(),
            unallocated_reward_key.clone(),
        )
        .await?;
    let (unallocated_server, unallocated_indexer) = iceberg::create::<IcebergUnallocatedReward>(
        catalog,
        iceberg_settings,
        pool,
        unallocated_reward_key.clone(),
    )
    .await?;

    TaskManager::builder()
        .add_task(data_transfer_server)
        .add_task(data_transfer_indexer)
        .add_task(service_provider_server)
        .add_task(service_provider_indexer)
        .add_task(unallocated_server)
        .add_task(unallocated_indexer)
        .build()
        .start()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
