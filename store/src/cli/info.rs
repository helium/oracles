use crate::{cli::print_json, datetime_from_epoch, Error, FileSource, FileType, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::StreamExt;
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
        let file_source = FileSource::new(&self.path)?;
        let file_info = file_source.file_info.clone();
        let mut file_stream = file_source.into_stream().await?;

        let mut count = 1;
        let buf = match file_stream.next().await {
            Some(buf) => buf?,
            None => {
                return Err(Error::not_found("no message found in file source"));
            }
        };

        let first_timestamp = get_timestamp(&file_info.file_type, &buf)?;
        {
            let mut last_buf: Option<BytesMut> = None;
            while let Some(buf) = file_stream.next().await {
                last_buf = Some(buf?);
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
