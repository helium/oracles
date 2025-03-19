use anyhow::Result;
use clap::Parser;
use reward_index::{
    cli::{escrow, server},
    settings::Settings,
};
use std::path::PathBuf;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Indexer")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    /// Run the reward index server
    Server(server::Server),
    /// Escrow Duration related commands
    Escrow(escrow::Escrow),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> anyhow::Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
            Self::Escrow(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
