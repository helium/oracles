use crate::{
    cli::print_json, datetime_from_naive, keypair::load_from_file, receipt_txn::handle_report_msg,
    Result,
};
use chrono::NaiveDateTime;
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::Message;
use serde_json::json;
use std::path::PathBuf;
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
};

/// Generate poc rewards
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
    /// Optional, write output to file
    #[clap(long)]
    write: Option<bool>,
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

        let poc_injector_kp_path = std::env::var("POC_IOT_INJECTOR_KEYPAIR")
            .unwrap_or_else(|_| String::from("/tmp/poc_iot_injector_keypair"));
        let poc_iot_injector_keypair = load_from_file(&poc_injector_kp_path)?;

        match self.write {
            Some(true) => {
                let outpath: PathBuf = [
                    "output_",
                    &after_utc.timestamp().to_string(),
                    "_",
                    &before_utc.timestamp().to_string(),
                    ".log",
                ]
                .iter()
                .collect();

                let outfile = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(outpath)
                    .await?;

                let mut wtr = BufWriter::new(outfile);

                while let Some(Ok(msg)) = stream.next().await {
                    if let Ok(Some((txn, _hash, _hash_b64_url))) =
                        handle_report_msg(msg, &poc_iot_injector_keypair, ts)
                    {
                        wtr.write_all(&txn.encode_to_vec()).await?;
                    }
                }

                wtr.flush().await?;

                Ok(())
            }
            _ => {
                while let Some(Ok(msg)) = stream.next().await {
                    if let Ok(Some((txn, _hash, _hash_b64_url))) =
                        handle_report_msg(msg, &poc_iot_injector_keypair, ts)
                    {
                        print_json(&json!({ "txn": txn }))?;
                    }
                }
                Ok(())
            }
        }
    }
}
