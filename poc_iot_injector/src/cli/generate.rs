use crate::{receipt_txn::handle_report_msg, Result, Settings};
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
    pub async fn run(&self, settings: &Settings) -> Result {
        let store = FileStore::from_settings(&settings.verifier).await?;

        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let file_list = store
            .list_all(FileType::LoraValidPoc, after_utc, before_utc)
            .await?;

        let before_ts = before_utc.timestamp_millis();

        let poc_oracle_key = settings.keypair()?;
        let shared_key = Arc::new(poc_oracle_key);

        store
            .source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed())
            .for_each_concurrent(STORE_WORKERS, |msg| {
                let shared_key_clone = shared_key.clone();
                async move {
                    match msg {
                        Ok(m) => process_msg(m, shared_key_clone, before_ts).await,
                        Err(e) => tracing::error!("unable to process msg due to: {:?}", e),
                    }
                }
            })
            .await;

        Ok(())
    }
}

async fn process_msg(msg: prost::bytes::BytesMut, shared_key_clone: Arc<Keypair>, before_ts: i64) {
    if let Ok(Some((txn, _hash, _hash_b64_url))) =
        handle_report_msg(msg.clone(), shared_key_clone, before_ts)
    {
        let tx = BlockchainTxn {
            txn: Some(Txn::PocReceiptsV2(txn)),
        };

        tracing::debug!("txn_bin: {:?}", tx.encode_to_vec());
    } else {
        tracing::error!("unable to construct txn for msg {:?}", msg)
    }
}
