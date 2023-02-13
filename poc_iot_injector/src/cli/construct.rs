use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Settings,
};
use anyhow::bail;
use file_store::{file_source, iot_valid_poc::IotPoc, traits::MsgDecode};
use futures::stream::StreamExt;
use helium_crypto::Keypair;
use helium_proto::Message;
use std::{path::PathBuf, sync::Arc};

/// Construct raw poc receipt txns from a given iot_valid_poc file
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required path to iot_valid_poc file
    #[clap(long)]
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let mut file_stream = file_source::source([&self.in_path]);
        let poc_oracle_key = settings.keypair()?;
        let max_witnesses_per_receipt = settings.max_witnesses_per_receipt;
        let shared_key = Arc::new(poc_oracle_key);

        while let Some(result) = file_stream.next().await {
            let msg = result?;
            let _ = process_msg(msg, shared_key.clone(), max_witnesses_per_receipt).await;
        }

        Ok(())
    }
}

async fn process_msg(
    msg: prost::bytes::BytesMut,
    shared_key_clone: Arc<Keypair>,
    max_witnesses_per_receipt: u64,
) -> anyhow::Result<TxnDetails> {
    let iot_poc = IotPoc::decode(msg)?;
    if let Ok(txn_details) = handle_report_msg(iot_poc, shared_key_clone, max_witnesses_per_receipt)
    {
        tracing::debug!("txn_bin: {:?}", txn_details.txn.encode_to_vec());
        Ok(txn_details)
    } else {
        bail!("unable to construct txn for msg")
    }
}
