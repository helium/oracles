use clap::Parser;
use file_store::{
    cli::{bucket, dump, dump_mobile_rewards, info},
    Result,
};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Bucket Commands")]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Info(info::Cmd),
    Dump(dump::Cmd),
    Bucket(Box<bucket::Cmd>),
    DumpMobileRewards(dump_mobile_rewards::Cmd),
}

impl Cmd {
    pub async fn run(&self) -> Result {
        match self {
            Cmd::Info(cmd) => cmd.run().await,
            Cmd::Dump(cmd) => cmd.run().await,
            Cmd::Bucket(cmd) => cmd.run().await,
            Cmd::DumpMobileRewards(cmd) => cmd.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    cli.run().await
}
