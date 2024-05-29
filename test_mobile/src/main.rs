use anyhow::Result;
use clap::Parser;
use test_mobile::cli::{assignment, price};

#[derive(clap::Parser)]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        self.cmd.run().await
    }
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    Assignment(assignment::Cmd),
    Price(price::Cmd),
}

impl Cmd {
    pub async fn run(self) -> Result<()> {
        match self {
            Self::Assignment(cmd) => cmd.run().await,
            Self::Price(cmd) => cmd.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
