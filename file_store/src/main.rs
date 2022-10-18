use clap::Parser;
use file_store::{
    cli::{bucket, dump, info},
    Result,
};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Info(info::Cmd),
    Dump(dump::Cmd),
    Bucket(bucket::Cmd),
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,

    #[clap(long = "dotenv-path", short = 'e', default_value = ".env")]
    dotenv_path: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    dotenv::from_path(&cli.dotenv_path)?;

    match cli.cmd {
        Cmd::Info(cmd) => cmd.run().await,
        Cmd::Dump(cmd) => cmd.run().await,
        Cmd::Bucket(cmd) => cmd.run().await,
    }
}
