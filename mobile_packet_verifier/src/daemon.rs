use crate::{burner::Burner, settings::Settings};
use anyhow::{bail, Error, Result};
use file_store::{
    file_info_poller::FileInfoStream, file_source, file_upload,
    mobile_session::DataTransferSessionIngestReport, FileSinkBuilder, FileStore, FileType,
};
use futures_util::TryFutureExt;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::read_keypair_file;
use sqlx::{Pool, Postgres};
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

pub struct Daemon {
    pool: Pool<Postgres>,
    burner: Burner,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    burn_period: Duration,
}

impl Daemon {
    pub fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        burner: Burner,
    ) -> Self {
        Self {
            pool,
            burner,
            reports,
            burn_period: Duration::from_secs(60 * 60 * settings.burn_period as u64),
        }
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<()> {
        let mut burn_time = Instant::now() + self.burn_period;
        loop {
            tokio::select! {
                file = self.reports.recv() => {
                    let Some(file) = file else {
                        anyhow::bail!("FileInfoPoller sender was dropped unexpectedly");
                    };
                    tracing::info!("Verifying file: {}", file.file_info);
                    let ts = file.file_info.timestamp;
                    let mut transaction = self.pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;
                    crate::accumulate::accumulate_sessions(&mut transaction, ts, reports).await?;
                    transaction.commit().await?;
                },
                _ = sleep_until(burn_time) => {
                    // It's time to burn
                    self.burner.burn(&self.pool).await?;
                    burn_time = Instant::now() + self.burn_period;
                }
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::install_metrics();

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Set up the postgres pool:
        let (pool, conn_handler) = settings
            .database
            .connect("mobile-packet-verifier", shutdown_listener.clone())
            .await?;
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

        let burner = Burner::new(settings, valid_sessions, rpc_client, burn_keypair).await?;

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (reports, source_join_handle) =
            file_source::continuous_source::<DataTransferSessionIngestReport>()
                .db(pool.clone())
                .store(file_store)
                .file_type(FileType::DataTransferSessionIngestReport)
                .build()?
                .start(shutdown_listener.clone())
                .await?;

        let daemon = Daemon::new(settings, pool, reports, burner);

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
