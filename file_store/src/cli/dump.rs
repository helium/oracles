use crate::{
    cli::print_json,
    file_source,
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    iot_packet::IotValidPacket,
    mobile_session::{DataTransferSessionIngestReport, InvalidDataTransferIngestReport},
    mobile_subscriber::{SubscriberLocationIngestReport, VerifiedSubscriberLocationIngestReport},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::MsgDecode,
    FileType, Result, Settings,
};
use base64::Engine;
use csv::Writer;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::{
        packet_verifier::ValidDataTransferSession as ValidDataTransferSessionProto,
        poc_lora::{
            LoraBeaconIngestReportV1, LoraInvalidWitnessReportV1, LoraPocV1,
            LoraWitnessIngestReportV1,
        },
        poc_mobile::{
            mobile_reward_share::Reward, CellHeartbeatIngestReportV1, CellHeartbeatReqV1,
            Heartbeat, InvalidDataTransferIngestReportV1, MobileRewardShare, RadioRewardShare,
            SpeedtestAvg, SpeedtestIngestReportV1, SpeedtestReqV1,
        },
        router::PacketRouterPacketReportV1,
    },
    BlockchainTxn, Message, PriceReportV1, RewardManifest, SubnetworkRewards,
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
    pub async fn run(&self, _settings: &Settings) -> Result {
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
                FileType::DataTransferSessionIngestReport => {
                    let dtr = DataTransferSessionIngestReport::decode(msg)?;
                    print_json(&json!({
                        "received_timestamp": dtr.received_timestamp,
                        "reward_cancelled": dtr.report.reward_cancelled,
                        "pub_key": dtr.report.data_transfer_usage.pub_key,
                        "upload_bytes": dtr.report.data_transfer_usage.upload_bytes,
                        "download_bytes": dtr.report.data_transfer_usage.download_bytes,
                        "radio_access_technology": dtr.report.data_transfer_usage.radio_access_technology,
                        "event_id": dtr.report.data_transfer_usage.event_id,
                        "payer": dtr.report.data_transfer_usage.payer,
                        "timestamp": dtr.report.data_transfer_usage.timestamp,
                    }))?;
                }
                FileType::InvalidDataTransferSessionIngestReport => {
                    let msg: InvalidDataTransferIngestReport =
                        InvalidDataTransferIngestReportV1::decode(msg)?.try_into()?;
                    print_json(&json!({
                        "invalid_reason": msg.reason,
                        "invalid_timestamp": msg.timestamp,
                        "received_timestamp": msg.report.received_timestamp,
                        "reward_cancelled": msg.report.report.reward_cancelled,
                        "hotspot_key": PublicKey::try_from(msg.report.report.data_transfer_usage.pub_key)?,
                        "upload_bytes": msg.report.report.data_transfer_usage.upload_bytes,
                        "download_bytes": msg.report.report.data_transfer_usage.download_bytes,
                        "radio_access_technology": msg.report.report.data_transfer_usage.radio_access_technology,
                        "event_id": msg.report.report.data_transfer_usage.event_id,
                        "payer":  PublicKey::try_from(msg.report.report.data_transfer_usage.payer)?,
                        "event_timestamp": msg.report.report.data_transfer_usage.timestamp,
                    }))?;
                }
                FileType::ValidDataTransferSession => {
                    let msg = ValidDataTransferSessionProto::decode(msg)?;
                    print_json(&json!({
                        "pub_key": PublicKey::try_from(msg.pub_key)?,
                        "upload_bytes": msg.upload_bytes,
                        "download_bytes": msg.download_bytes,
                        "num_dcs": msg.num_dcs,
                        "upload_bytes": msg.upload_bytes,
                        "payer": PublicKey::try_from(msg.payer)?,
                        "first_timestamp": msg.first_timestamp,
                        "last_timestamp": msg.last_timestamp,
                    }))?;
                }
                FileType::IotBeaconIngestReport => {
                    let dec_msg = LoraBeaconIngestReportV1::decode(msg)?;
                    let json = json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(IotBeaconIngestReport::try_from(dec_msg)?)?;
                }
                FileType::IotWitnessIngestReport => {
                    let dec_msg = LoraWitnessIngestReportV1::decode(msg)?;
                    let json = json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(IotWitnessIngestReport::try_from(dec_msg)?)?;
                }
                FileType::IotInvalidWitnessReport => {
                    let dec_msg = LoraInvalidWitnessReportV1::decode(msg)?;
                    let json = json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "reason":  dec_msg.reason
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(IotWitnessIngestReport::try_from(dec_msg)?)?;
                }
                FileType::IotPoc => {
                    let dec_msg = LoraPocV1::decode(msg)?;
                    let json = json!({
                        "poc_id": dec_msg.poc_id,
                        "beacon_report":  dec_msg.beacon_report,
                        "selected_witnesses": dec_msg.selected_witnesses,
                        "unselected_witnesses": dec_msg.unselected_witnesses,
                    });
                    // TODO: tmp dump out as json
                    // printing to json here as csv serializing failing due on header generation from struct
                    print_json(&json)?;
                    // wtr.serialize(IotValidPoc::try_from(dec_msg)?)?;
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
                FileType::SpeedtestAvg => {
                    let speedtest_avg = SpeedtestAvg::decode(msg)?;
                    print_json(&json!({
                        "pub_key": PublicKey::try_from(speedtest_avg.pub_key)?,
                        "upload_speed_avg_bps": speedtest_avg.upload_speed_avg_bps,
                        "download_speed_avg_bps": speedtest_avg.download_speed_avg_bps,
                        "latency_avg_ms": speedtest_avg.latency_avg_ms,
                        "validity": speedtest_avg.validity,
                        "number_of_speedtests": speedtest_avg.speedtests.len(),
                        "reward_multiplier": speedtest_avg.reward_multiplier,
                    }))?;
                }
                FileType::ValidatedHeartbeat => {
                    let heartbeat = Heartbeat::decode(msg)?;
                    print_json(&json!({
                        "cbsd_id": heartbeat.cbsd_id,
                        "pub_key": PublicKey::try_from(heartbeat.pub_key)?,
                        "reward_multiplier": heartbeat.reward_multiplier,
                        "timestamp": heartbeat.timestamp,
                        "cell_type": heartbeat.cell_type,
                        "validity": heartbeat.validity,
                    }))?;
                }
                FileType::MobileRewardShare => {
                    let reward = MobileRewardShare::decode(msg)?;
                    match reward.reward {
                        Some(Reward::GatewayReward(reward)) => print_json(&json!({
                            "hotspot_key": PublicKey::try_from(reward.hotspot_key)?,
                            "dc_transfer_reward": reward.dc_transfer_reward,
                        }))?,
                        Some(Reward::RadioReward(reward)) => print_json(&json!({
                            "cbsd_id": reward.cbsd_id,
                            "poc_reward": reward.poc_reward,
                        }))?,
                        Some(Reward::SubscriberReward(reward)) => print_json(&json!({
                            "subscriber_id": reward.subscriber_id,
                            "discovery_location_amount": reward.discovery_location_amount,
                        }))?,
                        _ => (),
                    }
                }
                FileType::RadioRewardShare => {
                    let reward = RadioRewardShare::decode(msg)?;
                    print_json(&json!({
                        "owner_key": PublicKey::try_from(reward.owner_key)?,
                        "hotpost_key": PublicKey::try_from(reward.hotspot_key)?,
                        "cbsd_id": reward.cbsd_id,
                        "amount": reward.amount,
                        "start_epoch": reward.start_epoch,
                        "end_epoch": reward.end_epoch,
                    }))?;
                }
                FileType::RewardManifest => {
                    let manifest = RewardManifest::decode(msg)?;
                    print_json(&json!({
                        "written_files": manifest.written_files,
                        "start_timestamp": manifest.start_timestamp,
                        "end_timestamp": manifest.end_timestamp,
                    }))?;
                }
                FileType::SignedPocReceiptTxn => {
                    // This just outputs a binary of the txns instead of the typical decode.
                    // This is to make ingesting the output of these transactions simpler on chain.
                    let wrapped_txn = BlockchainTxn::decode(msg)?;
                    println!("{:?}", wrapped_txn.encode_to_vec());
                }
                FileType::IotPacketReport => {
                    let packet_report = PacketRouterPacketReportV1::decode(msg)?;
                    print_json(&json!({
                        "oui": packet_report.oui,
                        "timestamp": packet_report.gateway_tmst}))?;
                }
                FileType::PriceReport => {
                    let manifest = PriceReportV1::decode(msg)?;
                    print_json(&json!({
                        "price": manifest.price,
                        "timestamp": manifest.timestamp,
                        "token_type": manifest.token_type(),
                    }))?;
                }
                FileType::IotValidPacket => {
                    let manifest = IotValidPacket::decode(msg)?;
                    print_json(&json!({
                        "payload_size": manifest.payload_size,
                        "gateway": PublicKey::try_from(manifest.gateway)?,
                        "payload_hash": base64::engine::general_purpose::STANDARD.encode(manifest.payload_hash),
                        "num_dcs": manifest.num_dcs,
                        "packet_timestamp": manifest.packet_timestamp,
                    }))?;
                }
                FileType::SubscriberLocationIngestReport => {
                    let report = SubscriberLocationIngestReport::decode(msg)?;
                    print_json(&json!({
                        "subscriber_id": report.report.subscriber_id,
                        "carrier_pub_key": report.report.carrier_pub_key,
                        "recv_timestamp": report.received_timestamp}))?;
                }
                FileType::VerifiedSubscriberLocationIngestReport => {
                    let report = VerifiedSubscriberLocationIngestReport::decode(msg)?;
                    print_json(&json!({
                        "subscriber_id": report.report.report.subscriber_id,
                        "carrier_pub_key": report.report.report.carrier_pub_key,
                        "status": report.status,
                        "recv_timestamp": report.report.received_timestamp}))?;
                }
                _ => (),
            }
        }

        wtr.flush()?;

        Ok(())
    }
}
