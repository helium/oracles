use crate::{cli::print_json, datetime_from_epoch, Error, FileSource, FileType, Result};
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
    Message,
};
use serde_json::json;
use std::path::PathBuf;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path to store file
    path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let file_size = self.path.metadata()?.len();
        let mut file_source = FileSource::new(&self.path).await?;
        let file_type = file_source.file_type.clone();

        let mut count = 1;
        let mut buf = match file_source.read().await? {
            Some(buf) => buf,
            None => {
                return Err(Error::not_found("no message found in file source"));
            }
        };

        let first_timestamp = match file_type {
            FileType::CellHeartbeat => CellHeartbeatReqV1::decode(&mut buf)
                .map(|entry| datetime_from_epoch(entry.timestamp))?,
            FileType::CellSpeedtest => SpeedtestReqV1::decode(&mut buf)
                .map(|entry| datetime_from_epoch(entry.timestamp))?,
        };

        loop {
            let buf = file_source.read().await?;
            if buf.is_none() {
                break;
            }
            count += 1;
        }

        let json = json!({
            "file_size": file_size,
            "file_type": file_type.to_string(),
            "first_timestamp":  first_timestamp,
            "count": count,
        });
        print_json(&json)
    }
}
