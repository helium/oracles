use clap::Parser;
use poc_ingest::{server_5g, server_lora, Result};
use tokio::{self, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Ingest Server")]
pub struct Cli {}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "poc_ingest=debug,poc_store=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _cli = Cli::parse();

    // Install the prometheus metrics exporter
    poc_metrics::install_metrics();

    // configure shutdown trigger
    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // run the grpc server in either lora or mobile 5g mode
    let server_mode = dotenv::var("GRPC_SERVER_MODE")?;
    match server_mode.as_str() {
        "lora" => server_lora::grpc_server(shutdown_listener, server_mode).await,
        "mobile" => server_5g::grpc_server(shutdown_listener, server_mode).await,
        _ =>
        //TODO: return proper error here
        {
            std::process::exit(9)
        }
    }
}
