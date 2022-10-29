use clap::Parser;
use poc_mobile_verifier::{
    cli::{generate, reward_from_db, server},
    Result, Settings,
};
use std::path;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    pub async fn run(self) -> Result {
        let settings = Settings::new(self.config)?;
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();
        self.cmd.run(settings).await
    }
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    Generate(generate::Cmd),
    Server(server::Cmd),
    RewardFromDb(reward_from_db::Cmd),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result {
        match self {
            Self::Generate(cmd) => cmd.run(&settings).await,
            Self::Server(cmd) => cmd.run(&settings).await,
            Self::RewardFromDb(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    cli.run().await
}
