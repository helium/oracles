use crate::{file_source, heartbeat::CellHeartbeat, Result};
use csv::Writer;
use futures::stream::StreamExt;
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
        let mut file_stream = file_source::source(&[&self.in_path]);

        let mut wtr = Writer::from_writer(io::stdout());
        while let Some(result) = file_stream.next().await {
            let msg = result?;
            let dec_msg = CellHeartbeatReqV1::decode(msg)?;
            wtr.serialize(CellHeartbeat::try_from(dec_msg)?)?;
        }

        wtr.flush()?;

        Ok(())
    }
}
