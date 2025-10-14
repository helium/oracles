use clap::Parser;

use ingest_mobile::{server, settings::Settings};

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let settings = Settings::new(cli.config)?;
    tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);

    poc_metrics::start_metrics(&settings.metrics)?;
    custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;

    server::grpc_server(settings).await
}
