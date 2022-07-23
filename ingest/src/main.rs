use clap::Parser;
use poc5g_ingest::{server, Result};
use tokio::{self, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Ingest Server")]
pub struct Cli {}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "poc5g_ingest=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _cli = Cli::parse();

    // configure shutdown trigger
    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // api server
    let api_server = server::api_server(shutdown_listener.clone());

    // grpc server
    let grpc_server = server::grpc_server(shutdown_listener.clone());

    tokio::try_join!(api_server, grpc_server,)?;

    Ok(())
}
