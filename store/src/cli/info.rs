use crate::{cli::print_json, datetime_from_epoch, Error, FileSource, FileType, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
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
        let file_info = file_source.file_info.clone();

        let mut count = 1;
        let buf = match file_source.read().await? {
            Some(buf) => buf,
            None => {
                return Err(Error::not_found("no message found in file source"));
            }
        };

        let first_timestamp = get_timestamp(&file_info.file_type, &buf)?;
        {
            let mut last_buf: Option<BytesMut> = None;
            loop {
                let buf = file_source.read().await?;
                if buf.is_none() {
                    break;
                } else {
                    last_buf = buf;
                }
                count += 1;
            }

            let last_timestamp = if let Some(buf) = last_buf {
                Some(get_timestamp(&file_info.file_type, &buf)?)
            } else {
                None
            };

            let json = json!({
                "file": json!({
                    "size": file_size,
                    "type": file_info.file_type.to_string(),
                    "timestamp": file_info.file_timestamp,
                }),
                "first_timestamp":  first_timestamp,
                "last_timestamp": last_timestamp,
                "count": count,
            });
            print_json(&json)
        }
    }
}

fn get_timestamp(file_type: &FileType, buf: &[u8]) -> Result<DateTime<Utc>> {
    let result = match file_type {
        FileType::CellHeartbeat => {
            CellHeartbeatReqV1::decode(buf).map(|entry| datetime_from_epoch(entry.timestamp))?
        }
        FileType::CellSpeedtest => {
            SpeedtestReqV1::decode(buf).map(|entry| datetime_from_epoch(entry.timestamp))?
        }
    };
    Ok(result)
}
