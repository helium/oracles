use clap::Parser;
use poc_store::{cli::info, Result};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Info(info::Cmd),
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Ingest Server")]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Info(cmd) => cmd.run().await,
    }
}
