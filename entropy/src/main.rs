use chrono::Duration;
use clap::Parser;
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use poc_entropy::{entropy_generator::EntropyGenerator, server::ApiServer, Error, Result};
use std::path;
use tokio::{self, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const ENTROPY_SINK_ROLL_MINS: i64 = 5;

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Entropy Server")]
pub struct Cli {}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "poc_entropy=debug,poc_store=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _cli = Cli::parse();

    // Install the prometheus metrics exporter
    poc_metrics::install_metrics();

    // configure shutdown trigger
    let (shutdown_trigger, shutdown) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload = file_upload::FileUpload::from_env(file_upload_rx).await?;

    let store_path =
        std::env::var("ENTROPY_STORE").unwrap_or_else(|_| String::from("/var/data/entropy"));
    let store_base_path = path::Path::new(&store_path);

    // entropy
    let mut entropy_generator = EntropyGenerator::from_env().await?;
    let entropy_watch = entropy_generator.receiver();

    let (entropy_tx, entropy_rx) = file_sink::message_channel(50);
    let mut entropy_sink =
        file_sink::FileSinkBuilder::new(FileType::EntropyReport, store_base_path, entropy_rx)
            .deposits(Some(file_upload_tx.clone()))
            .roll_time(Duration::minutes(ENTROPY_SINK_ROLL_MINS))
            .create()
            .await?;

    // server
    let api_server = ApiServer::from_env(entropy_watch).await?;

    tracing::info!("api listening on {}", api_server.socket_addr);

    tokio::try_join!(
        api_server.run(&shutdown),
        entropy_sink.run(&shutdown).map_err(Error::from),
        entropy_generator.run(entropy_tx, &shutdown),
        file_upload.run(&shutdown).map_err(Error::from),
    )
    .map(|_| ())
}
