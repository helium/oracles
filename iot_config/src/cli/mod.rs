use crate::{cli::daemon::Daemon, Settings};
use std::path::PathBuf;

pub mod daemon;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium IoT Config Service")]
pub struct Cli {
    /// Optional configuration file to use. If present, the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.cmd {
            Cmd::Server(server) => {
                let settings = Settings::new(self.config)?;
                server.run(&settings).await
            }
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Daemon),
}
