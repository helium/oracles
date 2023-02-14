use std::path;

use clap::Parser;
use denylist::{cli::check, Settings};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Check(check::Cmd),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> anyhow::Result<()> {
        match self {
            Self::Check(cmd) => {
                let res = cmd.run(&settings).await?;
                println!("{:?}", res);
                Ok(())
            }
        }
    }
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Denylist Checker")]
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
    pub async fn run(self) -> anyhow::Result<()> {
        let settings = Settings::new(self.config)?;
        self.cmd.run(settings).await
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
