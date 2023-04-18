use anyhow::Result;
use clap::Parser;
use jemallocator::Jemalloc;
use mobile_packet_verifier::{daemon, settings::Settings};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// We use jemalloc due to high allocation churn and fragmentation
// when using the system allocator.
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Packer Verifier Server")]
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
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();
        self.cmd.run(settings).await
    }
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    Server(daemon::Cmd),
}

impl Cmd {
    async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
