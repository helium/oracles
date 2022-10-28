use crate::{
    keypair::{load_from_file, Keypair},
    receipt_txn::handle_report_msg,
    Error, Result, LOADER_WORKERS, STORE_WORKERS,
};
use chrono::{NaiveDateTime, TimeZone, Utc};
use file_store::{file_sink, file_sink_write, file_upload, FileStore, FileType};
use futures::stream::{self, StreamExt};
use futures_util::TryFutureExt;
use helium_proto::{blockchain_txn::Txn, BlockchainTxn, Message};
use std::sync::Arc;

/// Generate poc rewards
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
    /// Optional flag to save txns to S3 bucket
    #[clap(long)]
    save: bool,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let mut store = FileStore::from_env_with_prefix("INPUT").await?;

        let poc_injector_kp_path = std::env::var("POC_IOT_ORACLE_KEY")
            .unwrap_or_else(|_| String::from("/tmp/poc_iot_oracle_key"));
        let poc_oracle_key = load_from_file(&poc_injector_kp_path)?;
        let shared_key = Arc::new(poc_oracle_key);

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_env_with_prefix("OUTPUT", file_upload_rx).await?;

        let poc_iot_rewards_store = std::env::var("POC_IOT_REWARDS_STORE")
            .unwrap_or_else(|_| String::from("/tmp/poc_iot_rewards_store"));
        let store_base_path = std::path::Path::new(&poc_iot_rewards_store);

        // file sink for poc receipts
        let (sender, receiver) = file_sink::message_channel(50);
        let mut sink = file_sink::FileSinkBuilder::new(
            FileType::SignedPocReceiptTxn,
            store_base_path,
            receiver,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        tokio::try_join!(
            sink.run(&shutdown_listener).map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            self.handle_stream(&mut store, shared_key, sender)
        )?;

        Ok(())
    }

    async fn handle_stream(
        &self,
        store: &mut FileStore,
        shared_key: Arc<Keypair>,
        sender: file_sink::MessageSender,
    ) -> Result<()> {
        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let file_list = store
            .list_all(FileType::LoraValidPoc, after_utc, before_utc)
            .await?;

        let before_ts = before_utc.timestamp_millis();

        store
            .source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed())
            .for_each_concurrent(STORE_WORKERS, |msg| {
                let shared_key_clone = shared_key.clone();
                let shared_sender = sender.clone();
                async move {
                    match msg {
                        Ok(m) => {
                            if let Some(txn) = process_msg(m, shared_key_clone, before_ts).await {
                                if self.save {
                                    tracing::debug!("writing txn to s3");
                                    // write txn to s3 bucket
                                    let _ = file_sink_write!(
                                        "signed_poc_receipt_txn",
                                        &shared_sender,
                                        txn
                                    )
                                    .await
                                    .unwrap()
                                    .await;
                                }
                            }
                        }
                        Err(e) => tracing::error!("unable to process msg due to: {:?}", e),
                    }
                }
            })
            .await;
        tracing::info!("completed processing stream");
        Ok(())
    }
}

async fn process_msg(
    msg: prost::bytes::BytesMut,
    shared_key_clone: Arc<Keypair>,
    before_ts: i64,
) -> Option<BlockchainTxn> {
    if let Ok(Some((txn, _hash, _hash_b64_url))) =
        handle_report_msg(msg.clone(), shared_key_clone, before_ts)
    {
        let tx = BlockchainTxn {
            txn: Some(Txn::PocReceiptsV2(txn)),
        };

        tracing::debug!("txn_bin: {:?}", tx.encode_to_vec());
        return Some(tx);
    } else {
        tracing::error!("unable to construct txn for msg {:?}", msg)
    }
    None
}
