use anyhow::{bail, Error, Result};
use clap::Parser;
use file_store::{
    file_source, file_upload, mobile_session::DataTransferSessionIngestReport, FileSinkBuilder,
    FileStore, FileType,
};
use futures_util::TryFutureExt;
use mobile_packet_verifier::{burner::Burner, daemon::Daemon, settings::Settings};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::read_keypair_file;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium IOT Packer Verifier Server")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();
        poc_metrics::install_metrics();

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Set up the postgres pool:
        let (pool, conn_handler)  = settings.database.connect("mobile-packet-verifier", shutdown_listener.clone()).await?;
        sqlx::migrate!().run(&pool).await?;

        // Set up the solana RpcClient:
        let rpc_client = RpcClient::new(settings.solana_rpc.clone());

        // Set up the balance burner:
        let burn_keypair = match read_keypair_file(&settings.burn_keypair) {
            Ok(kp) => kp,
            Err(e) => bail!("Failed to read keypair file ({})", e),
        };

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        let (valid_sessions, mut valid_sessions_server) = FileSinkBuilder::new(
            FileType::ValidDataTransferSession,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_invalid_packets"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(true)
        .create()
        .await?;

        let burner = Burner::new(&settings, valid_sessions, rpc_client, burn_keypair).await?;

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (reports, source_join_handle) =
            file_source::continuous_source::<DataTransferSessionIngestReport>()
                .db(pool.clone())
                .store(file_store)
                .file_type(FileType::DataTransferSessionIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        let daemon = Daemon::new(&settings, pool, reports, burner);

        tokio::try_join!(
            source_join_handle.map_err(Error::from),
            valid_sessions_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            daemon.run(&shutdown_listener).map_err(Error::from),
            conn_handler.map_err(Error::from),
        )?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
