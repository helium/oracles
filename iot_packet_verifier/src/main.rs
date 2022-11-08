use clap::Parser;
use iot_packet_verifier::{loader, runner, Result, Settings};
use std::path;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    /// Optional settings.toml file for configuration.
    /// Environemnt vars override settings in .toml file.
    #[clap(short='c')]
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

#[derive(clap::Subcommand, Debug)]
pub enum Cmd {
    /// Stream processing of IoT Packet Reports in S3 from HPR
    Server(Server),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result {
        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let runner = runner::Runner::from_settings(settings).await?;
        let loader = loader::Loader::from_settings(settings).await?;
        tokio::try_join!(
            runner.run(&shutdown_listener),
            loader.run(&shutdown_listener),
        )
        .map(|_| ())
    }
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    cli.run().await
}
