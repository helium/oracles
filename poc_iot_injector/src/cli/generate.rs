use crate::{datetime_from_naive, keypair::load_from_file, receipt_txn::handle_report_msg, Result};
use chrono::NaiveDateTime;
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{blockchain_txn::Txn, BlockchainTxn, Message};

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

        let after_utc = datetime_from_naive(self.after);
        let before_utc = datetime_from_naive(self.before);

        let file_list = store
            .list_all(FileType::LoraValidPoc, after_utc, before_utc)
            .await?;

        let mut stream = store.source(stream::iter(file_list).map(Ok).boxed());
        let ts = before_utc.timestamp_millis();

        let poc_injector_kp_path =
            std::env::var("POC_ORACLE_KEY").unwrap_or_else(|_| String::from("/tmp/poc_oracle_key"));
        let poc_oracle_key = load_from_file(&poc_injector_kp_path)?;

        while let Some(Ok(msg)) = stream.next().await {
            if let Ok(Some((txn, _hash, _hash_b64_url))) =
                handle_report_msg(msg, &poc_oracle_key, ts)
            {
                let tx = BlockchainTxn {
                    txn: Some(Txn::PocReceiptsV2(txn)),
                };

                println!("txn_bin: {:?}", tx.encode_to_vec());
            }
        }
        Ok(())
    }
}
