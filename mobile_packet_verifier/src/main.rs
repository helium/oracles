use anyhow::{Error, Result};
use clap::Parser;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient, file_source, file_upload,
    iot_packet::PacketRouterPacketReport, mobile_session::DataTransferSessionIngestReport,
    FileSinkBuilder, FileStore, FileType,
};
use iot_mobile_verifier::{burner::Burner, settings::Settings, verifier::Verifier};
use solana_client::nonblocking::rpc_client::RpcClient;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
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

        // Set up the psotgres pool:
        let pool = settings.database.connect(10).await?;
        sqlx::migrat!().run(&pool).await?;

        // Set up the database lock:
        let db_lock = Arc::new(Mutex::new(()));

        // Set up the solana RpcClient:
        let rpc_client = Arc::new(RpcClient::new(settings.solana_rpc.clone()));

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
        .auto_commit(false)
        .create()
        .await?;

        let burner = Burner::new(
            &settings,
            pool.clone(),
            valid_sessions,
            db_lock.clone(),
            rpc_client,
            burn_keypair,
        )
        .await?;

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let (reports, source_join_handle) =
            file_source::continuous_source::<DataTransferSessionIngestReport>()
                .db(pool.clone())
                .store(file_store)
                .file_type(FileType::DataTransferSessionIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        let verifier = Verifier::new(pool, reports, db_lock);

        tokio::try_join!(
            source_join_handle.map_err(Error::from),
            verifier.run(&shutdown_listener).map_err(Error::from),
            burner.run(&shutdown_listener).map_err(Error::from),
            valid_sessions_server
                .run(&shutdown_listener)
                .map_err(Error::from),
        )?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
