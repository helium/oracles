#![deny(unsafe_code)]

use anyhow::Result;
use clap::Parser;
use mobile_packet_verifier::{backfill, daemon, settings::Settings};
use std::path::PathBuf;

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Packer Verifier Server")]
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

#[derive(clap::Subcommand)]
pub enum Cmd {
    Server(daemon::Cmd),
    Backfill(backfill::Cmd),
}

impl Cmd {
    async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
            Self::Backfill(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
