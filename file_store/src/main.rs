use clap::Parser;
use file_store::{
    cli::{bucket, dump, info},
    Result, Settings,
};
use std::path;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Bucket Commands")]
pub struct Cli {
    /// Configuration file to use.
    #[clap(short = 'c')]
    config: path::PathBuf,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result {
        let settings = Settings::new(&self.config)?;
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Info(info::Cmd),
    Dump(dump::Cmd),
    Bucket(Box<bucket::Cmd>),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        match self {
            Cmd::Info(cmd) => cmd.run(&settings).await,
            Cmd::Dump(cmd) => cmd.run(&settings).await,
            Cmd::Bucket(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    cli.run().await
}
