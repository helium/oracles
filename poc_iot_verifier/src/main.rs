use clap::Parser;
use density_scaler::Server as DensityScaler;
use futures::TryFutureExt;
use poc_iot_verifier::{loader, purger, runner, Error, Result, Settings};
use std::path;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium POC IOT Verifier")]
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
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Server),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool and run migrations
        let pool = settings.database.connect(2).await?;
        sqlx::migrate!().run(&pool).await?;

        // Create the density scaler query messaging channel
        let (density_tx, density_rx) = density_scaler::query_channel(50);

        // configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let mut loader = loader::Loader::from_settings(settings).await?;
        // let mut runner = runner::Runner::from_settings(settings).await?;
        // let purger = purger::Purger::from_settings(settings).await?;
        let mut density_scaler = DensityScaler::from_settings(settings.density_scaler.clone())?;
        tokio::try_join!(
            // runner.run(density_tx, &shutdown),
            loader.run(&shutdown),
            // purger.run(&shutdown),
            density_scaler
                .run(density_rx, &shutdown)
                .map_err(Error::from),
        )
        .map(|_| ())
    }
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    cli.run().await
}
