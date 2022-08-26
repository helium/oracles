use crate::{cli::print_json, datetime_from_epoch, file_source, Error, FileInfo, FileType, Result};
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
        let file_info = FileInfo::try_from(self.path.as_path())?;
        let mut file_stream = file_source::source(&[&self.path]);

        let mut count = 1;
        let buf = match file_stream.next().await {
            Some(Ok(buf)) => buf,
            Some(Err(err)) => return Err(err),
            None => {
                return Err(Error::not_found("no message found in file source"));
            }
        };

        let first_timestamp = get_timestamp(&file_info.file_type, &buf)?;
        {
            let mut last_buf: Option<BytesMut> = None;
            while let Some(result) = file_stream.next().await {
                let buf = result?;
                last_buf = Some(buf);
                count += 1;
            }

            let last_timestamp = if let Some(buf) = last_buf {
                Some(get_timestamp(&file_info.file_type, &buf)?)
            } else {
                None
            };

            let json = json!({
                "file": file_info,
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
