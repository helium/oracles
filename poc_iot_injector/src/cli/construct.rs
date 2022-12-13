use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Settings,
};
use anyhow::bail;
use file_store::file_source;
use futures::stream::StreamExt;
use helium_crypto::Keypair;
use helium_proto::Message;
use std::{path::PathBuf, sync::Arc};

/// Construct raw poc receipt txns from a given lora_valid_poc file
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required path to lora_valid_poc file
    #[clap(long)]
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let mut file_stream = file_source::source([&self.in_path]);
        let poc_oracle_key = settings.keypair()?;
        let shared_key = Arc::new(poc_oracle_key);

        while let Some(result) = file_stream.next().await {
            let msg = result?;
            process_msg(msg, shared_key.clone()).await?;
        }

        Ok(())
    }
}

async fn process_msg(
    msg: prost::bytes::BytesMut,
    shared_key_clone: Arc<Keypair>,
) -> anyhow::Result<TxnDetails> {
    if let Ok(txn_details) = handle_report_msg(msg.clone(), shared_key_clone) {
        tracing::debug!("txn_bin: {:?}", txn_details.txn.encode_to_vec());
        Ok(txn_details)
    } else {
        bail!("unable to construct txn for msg")
    }
}
