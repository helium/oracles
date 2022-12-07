use crate::{server::Server, Settings};
use anyhow::{Error, Result};
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect(2).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // file upload setup
        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        // path for cache
        let store_base_path = std::path::Path::new(&settings.cache);

        // poc receipt txns
        let (poc_receipt_txn_tx, poc_receipt_txn_rx) = file_sink::message_channel(50);
        let mut poc_receipts = file_sink::FileSinkBuilder::new(
            FileType::SignedPocReceiptTxn,
            store_base_path,
            poc_receipt_txn_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(settings, poc_receipt_txn_tx).await?;

        tokio::try_join!(
            poc_receipts.run(&shutdown_listener).map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            poc_iot_injector_server
                .run(&shutdown_listener)
                .map_err(Error::from),
        )?;

        tracing::info!("Shutting down injector server");

        Ok(())
    }
}
