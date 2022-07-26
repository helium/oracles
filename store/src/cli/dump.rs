use crate::{heartbeat::CellHeartbeat, FileSource, Result};
use csv::Writer;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use std::io;
use std::path::PathBuf;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path for heartbeat/speedtest
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let mut file_source = FileSource::new(&self.in_path).await?;

        let mut wtr = Writer::from_writer(io::stdout());
        while let Some(msg) = file_source.read().await? {
            let dec_msg = CellHeartbeatReqV1::decode(msg)?;
            wtr.serialize(CellHeartbeat::try_from(dec_msg)?)?;
        }

        wtr.flush()?;

        Ok(())
    }
}
