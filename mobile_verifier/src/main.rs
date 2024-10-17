use anyhow::Result;
use clap::Parser;
use mobile_verifier::{
    cli::{reward_from_db, server, verify_disktree},
    Settings,
};
use std::path;

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Share Server")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<path::PathBuf>,

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

#[derive(clap::Subcommand)]
pub enum Cmd {
    Server(server::Cmd),
    RewardFromDb(reward_from_db::Cmd),
    /// Verify a Disktree file for HexBoosting.
    ///
    /// Go through every cell and ensure it's value can be turned into an Assignment.
    /// NOTE: This can take a very long time. Run with a --release binary.
    VerifyDisktree(verify_disktree::Cmd),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
            Self::RewardFromDb(cmd) => cmd.run(&settings).await,
            Self::VerifyDisktree(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
