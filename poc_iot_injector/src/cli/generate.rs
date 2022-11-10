use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Error, Result, Settings, LOADER_WORKERS, STORE_WORKERS,
};
use chrono::{NaiveDateTime, TimeZone, Utc};
use file_store::{FileStore, FileType};
use futures::{
    stream::{self, StreamExt},
    TryStreamExt,
};
use helium_crypto::Keypair;
use helium_proto::Message;
use std::sync::{Arc, Mutex};

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

        let success_counter = Arc::new(Mutex::new(0));
        let failure_counter = Arc::new(Mutex::new(0));

        store
            .source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed())
            .try_for_each_concurrent(STORE_WORKERS, |msg| {
                let shared_key_clone = shared_key.clone();
                let success_counter_ref = Arc::clone(&success_counter);
                let failure_counter_ref = Arc::clone(&failure_counter);
                async move {
                    if process_msg(msg, shared_key_clone, before_ts).await.is_ok() {
                        *success_counter_ref.lock().unwrap() += 1;
                    } else {
                        *failure_counter_ref.lock().unwrap() += 1;
                    }
                    Ok(())
                }
            })
            .await?;

        tracing::debug!("success_counter: {:?}", success_counter.lock().unwrap());
        tracing::debug!("failure_counter: {:?}", failure_counter.lock().unwrap());

        Ok(())
    }
}

async fn process_msg(
    msg: prost::bytes::BytesMut,
    shared_key_clone: Arc<Keypair>,
    before_ts: i64,
) -> Result<TxnDetails> {
    if let Ok(txn_details) = handle_report_msg(msg.clone(), shared_key_clone, before_ts) {
        tracing::debug!("txn_bin: {:?}", txn_details.txn.encode_to_vec());
        Ok(txn_details)
    } else {
        tracing::error!("unable to construct txn for msg {:?}", msg);
        Err(Error::TxnConstruction)
    }
}
