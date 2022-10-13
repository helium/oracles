use crate::{
    keypair::{load_from_file, Keypair},
    receipt_txn::handle_report_msg,
    Result,
};
use chrono::{NaiveDateTime, TimeZone, Utc};
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
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
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;

        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let file_list = store
            .list_all(FileType::LoraValidPoc, after_utc, before_utc)
            .await?;

        let stream = store.source(stream::iter(file_list).map(Ok).boxed());
        let before_ts = before_utc.timestamp_millis();

        let poc_injector_kp_path =
            std::env::var("POC_ORACLE_KEY").unwrap_or_else(|_| String::from("/tmp/poc_oracle_key"));
        let poc_oracle_key = load_from_file(&poc_injector_kp_path)?;
        let shared_key = Arc::new(poc_oracle_key);

        stream
            .for_each_concurrent(10, |msg| {
                let shared_key_clone = shared_key.clone();
                async move {
                    if let Ok(m) = msg {
                        process_msg(m, shared_key_clone, before_ts).await;
                    } else {
                        tracing::error!("unable to process msg: {:?}", msg)
                    }
                }
            })
            .await;

        Ok(())
    }
}

async fn process_msg(msg: prost::bytes::BytesMut, shared_key_clone: Arc<Keypair>, before_ts: i64) {
    if let Ok(Some((txn, _hash, _hash_b64_url))) =
        handle_report_msg(msg, shared_key_clone, before_ts)
    {
        let tx = BlockchainTxn {
            txn: Some(Txn::PocReceiptsV2(txn)),
        };

        tracing::debug!("txn_bin: {:?}", tx.encode_to_vec());
    }
}
