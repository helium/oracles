use clap::Parser;
use poc_iot_injector::{server::Server, Result};
use tokio::{self, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium POC IOT Injector Server")]
pub struct Cli {}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "poc_iot_injector=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _cli = Cli::parse();

    // Install the prometheus metrics exporter
    poc_metrics::install_metrics();

    // Configure shutdown trigger
    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // injector server keypair from env
    let injector_kp_path = std::env::var("INJECTOR_SERVER_KEYPAIR")
        .unwrap_or_else(|_| String::from("/tmp/injector_server_keypair"));
    let injector_keypair = poc_iot_injector::keypair::load_from_file(&injector_kp_path)?;

    // injector server
    let mut injector_server = Server::new(injector_keypair).await?;

    injector_server.run(shutdown_listener.clone()).await?;
    Ok(())
}
