use crate::{receipt_txn::handle_report_msg, Settings, LOADER_WORKERS};
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use file_store::{iot_valid_poc::IotPoc, traits::MsgDecode, FileStore, FileType};
use futures::{
    future,
    stream::{self, StreamExt},
};
use helium_proto::Message;
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
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        let store = FileStore::from_settings(&settings.verifier).await?;

        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let file_list = store
            .list_all(FileType::IotPoc, after_utc, before_utc)
            .await?;

        let poc_oracle_key = settings.keypair()?;
        let max_witnesses_per_receipt = settings.max_witnesses_per_receipt;
        let shared_key = Arc::new(poc_oracle_key);

        let mut success_counter = 0;
        let mut failure_counter = 0;

        let mut files = store
            .source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed())
            .filter_map(|x| future::ready(x.ok()));

        while let Some(msg) = files.next().await {
            let shared_key_clone = shared_key.clone();
            let iot_poc = IotPoc::decode(msg.clone())?;
            if let Ok(txn_details) =
                handle_report_msg(iot_poc, shared_key_clone, max_witnesses_per_receipt)
            {
                tracing::debug!("txn_bin: {:?}", txn_details.txn.encode_to_vec());
                success_counter += 1;
            } else {
                tracing::error!("unable to construct txn for msg {:?}", msg);
                failure_counter += 1;
            }
        }

        tracing::debug!("success_counter: {:?}", success_counter);
        tracing::debug!("failure_counter: {:?}", failure_counter);

        Ok(())
    }
}
