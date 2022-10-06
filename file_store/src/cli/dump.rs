use crate::{
    cli::print_json,
    file_source,
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    FileType, Result,
};
use csv::Writer;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::{
        poc_lora::{LoraBeaconIngestReportV1, LoraValidPocV1, LoraWitnessIngestReportV1},
        poc_mobile::{
            CellHeartbeatIngestReportV1, CellHeartbeatReqV1, Shares, SpeedtestIngestReportV1,
            SpeedtestReqV1,
        },
    },
    Message, SubnetworkRewards,
};
use serde_json::json;
use std::io;
use std::path::PathBuf;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Type of file to be dump
    file_type: FileType,
    /// Path to file
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let mut file_stream = file_source::source([&self.in_path]);

        let mut wtr = Writer::from_writer(io::stdout());
        while let Some(result) = file_stream.next().await {
            let msg = result?;
            match self.file_type {
                FileType::CellHeartbeat => {
                    let dec_msg = CellHeartbeatReqV1::decode(msg)?;
                    wtr.serialize(CellHeartbeat::try_from(dec_msg)?)?;
                }
                FileType::CellSpeedtest => {
                    let dec_msg = SpeedtestReqV1::decode(msg)?;
                    wtr.serialize(CellSpeedtest::try_from(dec_msg)?)?;
                }
                FileType::CellHeartbeatIngestReport => {
                    let dec_msg = CellHeartbeatIngestReportV1::decode(msg)?;
                    let ingest_report = CellHeartbeatIngestReport::try_from(dec_msg)?;
                    print_json(&ingest_report)?;
                }
                FileType::CellSpeedtestIngestReport => {
                    let dec_msg = SpeedtestIngestReportV1::decode(msg)?;
                    let ingest_report = CellSpeedtestIngestReport::try_from(dec_msg)?;
                    print_json(&ingest_report)?;
                }
                FileType::LoraBeaconIngestReport => {
                    let dec_msg = LoraBeaconIngestReportV1::decode(msg)?;
                    let json = json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(LoraBeaconIngestReport::try_from(dec_msg)?)?;
                }
                FileType::LoraWitnessIngestReport => {
                    let dec_msg = LoraWitnessIngestReportV1::decode(msg)?;
                    let json = json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(LoraWitnessIngestReport::try_from(dec_msg)?)?;
                }
                FileType::LoraValidPoc => {
                    let dec_msg = LoraValidPocV1::decode(msg)?;
                    let json = json!({
                        "poc_id": dec_msg.poc_id,
                        "beacon_report":  dec_msg.beacon_report,
                        "witnesses": dec_msg.witness_reports,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(LoraValidPoc::try_from(dec_msg)?)?;
                }
                FileType::InvalidShares => {
                    let dec_msg = Shares::decode(msg)?;
                    print_json(&dec_msg)?;
                }
                FileType::Shares => {
                    let dec_msg = Shares::decode(msg)?;
                    print_json(&dec_msg)?;
                }
                FileType::SubnetworkRewards => {
                    let proto_rewards = SubnetworkRewards::decode(msg)?.rewards;
                    let total_rewards = proto_rewards
                        .iter()
                        .fold(0, |acc, reward| acc + reward.amount);

                    let rewards: Vec<(PublicKey, u64)> = proto_rewards
                        .iter()
                        .map(|r| {
                            (
                                PublicKey::try_from(r.account.as_slice())
                                    .expect("unable to get public key"),
                                r.amount,
                            )
                        })
                        .collect();
                    print_json(&json!({ "rewards": rewards, "total_rewards": total_rewards }))?;
                }
                _ => (),
            }
        }

        wtr.flush()?;

        Ok(())
    }
}
