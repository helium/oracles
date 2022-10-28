use crate::{keypair::load_from_file, mk_db_pool, server::Server, Error, Result};
use file_store::{file_sink, file_upload, FileType};
use futures_util::TryFutureExt;
use std::path;
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
        // Install the prometheus metrics exporter
        poc_metrics::install_metrics();

        // Create database pool
        let pool = mk_db_pool(10).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Configure poc iot rewards store
        let store_path = std::env::var("POC_IOT_REWARDS_STORE")
            .unwrap_or_else(|_| String::from("/var/data/poc_iot_rewards_store"));
        let store_base_path = path::Path::new(&store_path);

        // Configure file_upload
        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_env_with_prefix("OUTPUT", file_upload_rx).await?;

        // Poc receipt txns
        let (poc_receipt_txn_tx, poc_receipt_txn_rx) = file_sink::message_channel(50);
        let mut poc_receipts = file_sink::FileSinkBuilder::new(
            FileType::SignedPocReceiptTxn,
            store_base_path,
            poc_receipt_txn_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        // injector server keypair from env
        let poc_injector_kp_path = std::env::var("POC_IOT_ORACLE_KEY")
            .unwrap_or_else(|_| String::from("/tmp/poc_iot_oracle_key"));
        let poc_oracle_key = load_from_file(&poc_injector_kp_path)?;

        // poc_iot_injector server
        let mut poc_iot_injector_server =
            Server::new(pool, poc_oracle_key, poc_receipt_txn_tx).await?;

        tokio::try_join!(
            poc_iot_injector_server.run(&shutdown).map_err(Error::from),
            poc_receipts.run(&shutdown).map_err(Error::from),
            file_upload.run(&shutdown).map_err(Error::from),
        )?;

        tracing::info!("Shutting down poc_iot_injector server");

        Ok(())
    }
}
