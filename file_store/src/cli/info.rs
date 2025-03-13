use crate::{
    cli::print_json,
    file_source,
    traits::{MsgTimestamp, TimestampDecode},
    Error, FileInfo, FileType, Result, Settings,
};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use helium_proto::services::poc_lora::{
    LoraBeaconIngestReportV1, LoraPocV1, LoraWitnessIngestReportV1,
};
use helium_proto::{
    services::poc_mobile::{SpeedtestIngestReportV1, SpeedtestReqV1},
    EntropyReportV1, Message, PriceReportV1,
};
use serde_json::json;
use std::{path::PathBuf, str::FromStr};

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path to store file
    path: PathBuf,
}

impl Cmd {
    pub async fn run(&self, _settings: &Settings) -> Result {
        let file_info = FileInfo::try_from(self.path.as_path())?;
        let mut file_stream = file_source::source([&self.path]);

        let mut count = 1;
        let buf = match file_stream.next().await {
            Some(Ok(buf)) => buf,
            Some(Err(err)) => return Err(err),
            None => {
                return Err(Error::not_found("no message found in file source"));
            }
        };

        let first_timestamp = get_timestamp(&file_info.prefix, &buf)?;
        {
            let mut last_buf: Option<BytesMut> = None;
            while let Some(result) = file_stream.next().await {
                let buf = result?;
                last_buf = Some(buf);
                count += 1;
            }

            let last_timestamp = if let Some(buf) = last_buf {
                Some(get_timestamp(&file_info.prefix, &buf)?)
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

impl MsgTimestamp<Result<DateTime<Utc>>> for PriceReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

fn get_timestamp(file_type: &str, buf: &[u8]) -> Result<DateTime<Utc>> {
    let result = match FileType::from_str(file_type)? {
        FileType::CellSpeedtest => SpeedtestReqV1::decode(buf)
            .map_err(Error::from)
            .and_then(|entry| entry.timestamp())?,
        FileType::CellSpeedtestIngestReport => SpeedtestIngestReportV1::decode(buf)
            .map_err(Error::from)
            .and_then(|ingest_report| {
                ingest_report.report.ok_or_else(|| {
                    Error::not_found("SpeedtestIngestReportV1 does not contain a SpeedtestReqV1")
                })
            })
            .and_then(|speedtest_req| speedtest_req.timestamp())?,
        FileType::EntropyReport => EntropyReportV1::decode(buf)
            .map_err(Error::from)
            .and_then(|entry| entry.timestamp())?,
        FileType::IotBeaconIngestReport => LoraBeaconIngestReportV1::decode(buf)
            .map_err(Error::from)
            .and_then(|entry| entry.timestamp())?,
        FileType::IotWitnessIngestReport => LoraWitnessIngestReportV1::decode(buf)
            .map_err(Error::from)
            .and_then(|entry| entry.timestamp())?,
        FileType::IotPoc => LoraPocV1::decode(buf)
            .map_err(Error::from)
            .and_then(|report| {
                report.beacon_report.ok_or_else(|| {
                    Error::not_found("IotValidPocV1 does not contain a IotBeaconIngestReportV1")
                })
            })
            .and_then(|beacon_report| beacon_report.timestamp())?,
        FileType::PriceReport => PriceReportV1::decode(buf)
            .map_err(Error::from)
            .and_then(|entry| entry.timestamp())?,

        _ => Utc::now(),
    };
    Ok(result)
}
