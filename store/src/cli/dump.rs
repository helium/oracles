use crate::{file_source, heartbeat::CellHeartbeat, speedtest::CellSpeedtest, FileType, Result};
use csv::Writer;
use futures::stream::StreamExt;
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
    Message,
};
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path for heartbeat/speedtest
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let mut file_stream = file_source::source(&[&self.in_path]);
        let mut wtr = Writer::from_writer(io::stdout());
        let file_prefix = self
            .in_path
            .file_name()
            .expect("invalid_file_name")
            .to_str()
            .expect("invalid_file_str")
            .split('.')
            .collect::<Vec<_>>()[0];

        match FileType::from_str(file_prefix)? {
            FileType::CellHeartbeat => {
                while let Some(result) = file_stream.next().await {
                    let msg = result?;
                    let dec_msg = CellHeartbeatReqV1::decode(msg)?;
                    wtr.serialize(CellHeartbeat::try_from(dec_msg)?)?;
                }
            }
            FileType::CellSpeedtest => {
                while let Some(result) = file_stream.next().await {
                    let msg = result?;
                    let dec_msg = SpeedtestReqV1::decode(msg)?;
                    wtr.serialize(CellSpeedtest::try_from(dec_msg)?)?;
                }
            }
        }

        wtr.flush()?;

        Ok(())
    }
}
