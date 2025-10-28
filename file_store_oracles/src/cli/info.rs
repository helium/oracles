use bytes::BytesMut;
use chrono::{DateTime, Utc};
use file_store::traits::TimestampDecode;
use file_store::{file_source, FileInfo};
use file_store_oracles::{traits::MsgTimestamp, FileType};
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

use super::print_json;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path to store file
    path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> anyhow::Result<()> {
        let file_info = FileInfo::try_from(self.path.as_path())?;
        let mut file_stream = file_source::source([&self.path]);

        let mut count = 1;

        let buf = match file_stream.next().await {
            Some(Ok(buf)) => buf,
            Some(Err(err)) => {
                anyhow::bail!("error streaming file {}: {err:?}", self.path.display());
            }
            None => {
                anyhow::bail!("no message found in file source")
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

fn get_timestamp(file_type: &str, buf: &[u8]) -> anyhow::Result<DateTime<Utc>> {
    let result = match FileType::from_str(file_type)? {
        FileType::CellSpeedtest => {
            let entry = SpeedtestReqV1::decode(buf)?;
            entry.timestamp()?
        }
        FileType::CellSpeedtestIngestReport => {
            let entry = SpeedtestIngestReportV1::decode(buf)?;
            let Some(speedtest_req) = entry.report else {
                anyhow::bail!("SpeedtestIngestReportV1 does not contain a SpeedtestReqV1");
            };
            speedtest_req.timestamp()?
        }
        FileType::EntropyReport => {
            let entry = EntropyReportV1::decode(buf)?;
            entry.timestamp()?
        }
        FileType::IotBeaconIngestReport => {
            let entry = LoraBeaconIngestReportV1::decode(buf)?;
            entry.timestamp()?
        }
        FileType::IotWitnessIngestReport => {
            let entry = LoraWitnessIngestReportV1::decode(buf)?;
            entry.timestamp()?
        }
        FileType::IotPoc => {
            let entry = LoraPocV1::decode(buf)?;
            let Some(beacon_report) = entry.beacon_report else {
                anyhow::bail!("IotValidPocV1 does not contain a IotBeaconIngestReportV1");
            };
            beacon_report.timestamp()?
        }
        FileType::PriceReport => {
            let entry = PriceReportV1::decode(buf)?;
            entry.timestamp.to_timestamp()?
        }

        _ => Utc::now(),
    };
    Ok(result)
}
