use std::path::PathBuf;

use base64::Engine;
use bs58;
use file_store::traits::TimestampDecode;
use futures::stream::StreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::{
    services::{
        chain_rewardable_entities::{
            entity_reward_destination_change_v1::RewardsDestination,
            split_recipient_info_v1::RewardAmount, EntityOwnershipChangeReportV1,
            EntityRewardDestinationChangeReportV1,
        },
        packet_verifier::ValidDataTransferSession as ValidDataTransferSessionProto,
        poc_lora::{
            iot_reward_share::Reward as IotReward, IotRewardShare as IotRewardShareProto,
            LoraBeaconIngestReportV1, LoraInvalidWitnessReportV1, LoraPocV1,
            LoraWitnessIngestReportV1,
        },
        poc_mobile::{
            mobile_reward_share::Reward as MobileReward, CoverageObjectV1, Heartbeat,
            MobileRewardShare, OracleBoostingReportV1, RadioRewardShare, SpeedtestAvg,
            SpeedtestIngestReportV1, UniqueConnectionsIngestReportV1,
            VerifiedUniqueConnectionsIngestReportV1,
        },
        router::PacketRouterPacketReportV1,
    },
    BlockchainTxn, BoostedHexUpdateV1 as BoostedHexUpdateProto, Message, PriceReportV1,
    SubnetworkRewards,
};
use serde_json::json;

use file_store::{file_source, traits::MsgDecode};
use file_store_oracles::{
    coverage::CoverageObject,
    iot_packet::IotValidPacket,
    mobile_radio_invalidated_threshold::VerifiedInvalidatedRadioThresholdIngestReport,
    mobile_radio_threshold::VerifiedRadioThresholdIngestReport,
    mobile_session::{
        DataTransferSessionIngestReport, InvalidDataTransferIngestReport,
        VerifiedDataTransferIngestReport,
    },
    mobile_subscriber::{SubscriberLocationIngestReport, VerifiedSubscriberLocationIngestReport},
    reward_manifest::RewardManifest,
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    unique_connections::UniqueConnectionReq,
    usage_counts::{HexUsageCountsIngestReport, RadioUsageCountsIngestReport},
    wifi_heartbeat::WifiHeartbeatIngestReport,
    FileType,
};

use super::print_json;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Type of file to be dump
    #[clap(short = 't')]
    file_type: FileType,
    /// Path to file
    #[clap(short = 'f')]
    in_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut file_stream = file_source::source([&self.in_path]);

        while let Some(result) = file_stream.next().await {
            let msg = result?;
            match self.file_type {
                FileType::HexUsageStatsIngestReport => {
                    let report = HexUsageCountsIngestReport::decode(msg)?;
                    print_json(&report)?;
                }
                FileType::RadioUsageStatsIngestReport => {
                    let report = RadioUsageCountsIngestReport::decode(msg)?;
                    print_json(&report)?;
                }
                FileType::VerifiedRadioThresholdIngestReport => {
                    let report = VerifiedRadioThresholdIngestReport::decode(msg)?;
                    print_json(&report)?;
                }
                FileType::VerifiedInvalidatedRadioThresholdIngestReport => {
                    let report = VerifiedInvalidatedRadioThresholdIngestReport::decode(msg)?;
                    print_json(&report)?;
                }
                FileType::BoostedHexUpdate => {
                    let dec_msg = BoostedHexUpdateProto::decode(msg)?;
                    let update = dec_msg.update.unwrap();
                    let json = json!({
                        "last_update": dec_msg.timestamp,
                        "location":  update.location,
                        "start_ts":  update.start_ts,
                        "end_ts":  update.end_ts,
                        "period_length":  update.period_length,
                        "multipliers":  update.multipliers,
                        "boosted_hex_pubkey":  update.boosted_hex_pubkey,
                        "boost_config_pubkey":  update.boost_config_pubkey,
                    });
                    print_json(&json)?;
                }
                FileType::WifiHeartbeatIngestReport => {
                    let msg = WifiHeartbeatIngestReport::decode(msg)?;
                    let json = json!({
                        "received_timestamp": msg.received_timestamp,
                        "pubkey": msg.report.pubkey,
                        "operation_mode": msg.report.operation_mode,
                        "location_validation_timestamp": msg.report.location_validation_timestamp,
                    });
                    print_json(&json)?;
                }
                FileType::CellSpeedtest => {
                    let msg = CellSpeedtest::decode(msg)?;
                    print_json(&json!({
                        "pubkey": msg.pubkey,
                        "serial": msg.serial,
                        "timestamp": msg.timestamp,
                        "upload_speed": msg.upload_speed,
                        "download_speed": msg.download_speed,
                        "latency": msg.latency,
                    }))?;
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
                        "rewardable_bytes": dtr.report.rewardable_bytes,
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
                    let msg = InvalidDataTransferIngestReport::decode(msg)?;
                    print_json(&json!({
                        "invalid_reason": msg.reason,
                        "invalid_timestamp": msg.timestamp,
                        "received_timestamp": msg.report.received_timestamp,
                        "rewardable_bytes": msg.report.report.rewardable_bytes,
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
                FileType::VerifiedDataTransferSession => {
                    let report = VerifiedDataTransferIngestReport::decode(msg)?;
                    let report = report.report;
                    let req = report.report;
                    print_json(&json!({
                        "rewardable_bytes": req.rewardable_bytes,
                        "pub_key": PublicKey::try_from(req.pub_key)?,
                        "received_timestamp": report.received_timestamp,
                        "data_transfer_usage": {
                            "pub_key": PublicKey::try_from(req.data_transfer_usage.pub_key)?,
                            "upload_bytes": req.data_transfer_usage.upload_bytes,
                            "download_bytes": req.data_transfer_usage.download_bytes,
                            "radio_access_technology": req.data_transfer_usage.radio_access_technology,
                            "event_id": req.data_transfer_usage.event_id,
                            "payer": req.data_transfer_usage.payer,
                            "timestamp": req.data_transfer_usage.timestamp,
                        },
                    }))?;
                }
                FileType::IotBeaconIngestReport => {
                    let dec_msg = LoraBeaconIngestReportV1::decode(msg)?;
                    print_json(&json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    }))?;
                }
                FileType::IotWitnessIngestReport => {
                    let dec_msg = LoraWitnessIngestReportV1::decode(msg)?;
                    print_json(&json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "report":  dec_msg.report,
                    }))?;
                }
                FileType::IotInvalidWitnessReport => {
                    let dec_msg = LoraInvalidWitnessReportV1::decode(msg)?;
                    print_json(&json!({
                        "received_timestamp": dec_msg.received_timestamp,
                        "reason":  dec_msg.reason
                    }))?;
                }
                FileType::IotPoc => {
                    let dec_msg = LoraPocV1::decode(msg)?;
                    print_json(&json!({
                        "poc_id": dec_msg.poc_id,
                        "beacon_report":  dec_msg.beacon_report,
                        "selected_witnesses": dec_msg.selected_witnesses,
                        "unselected_witnesses": dec_msg.unselected_witnesses,
                    }))?;
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
                        "timestamp": heartbeat.timestamp,
                        "cell_type": heartbeat.cell_type,
                        "validity": heartbeat.validity,
                    }))?;
                }
                FileType::IotRewardShare => {
                    let reward = IotRewardShareProto::decode(msg)?;
                    match reward.reward {
                        Some(IotReward::GatewayReward(reward)) => print_json(&json!({
                            "type": "gateway_reward",
                            "hotspot_key": PublicKey::try_from(reward.hotspot_key)?,
                            "dc_transfer_amount": reward.dc_transfer_amount,
                            "beacon_amount": reward.beacon_amount,
                            "witness_amount": reward.witness_amount,
                        }))?,
                        Some(IotReward::OperationalReward(reward)) => print_json(&json!({
                            "type": "operational_reward",
                            "amount": reward.amount,
                        }))?,
                        Some(IotReward::UnallocatedReward(reward)) => print_json(&json!({
                            "type": "unallocated_reward",
                            "unallocated_reward_type": reward.reward_type,
                            "amount": reward.amount,
                        }))?,
                        _ => (),
                    }
                }
                FileType::MobileRewardShare => {
                    let reward = MobileRewardShare::decode(msg)?;
                    let reward = reward.reward.expect("no reward found");
                    match reward {
                        MobileReward::GatewayReward(reward) => print_json(&json!({
                            "hotspot_key": PublicKey::try_from(reward.hotspot_key)?,
                            "dc_transfer_reward": reward.dc_transfer_reward,
                        }))?,
                        MobileReward::RadioReward(reward) => print_json(&json!({
                            "hotspot_key":  PublicKey::try_from(reward.hotspot_key)?,
                            "cbsd_id": reward.cbsd_id,
                            "poc_reward": reward.poc_reward,
                            "boosted_hexes": reward.boosted_hexes,
                        }))?,
                        MobileReward::SubscriberReward(reward) => print_json(&json!({
                            "subscriber_id": reward.subscriber_id,
                            "discovery_location_amount": reward.discovery_location_amount,
                            "verification_mapping_amount": reward.verification_mapping_amount,
                        }))?,
                        MobileReward::ServiceProviderReward(reward) => print_json(&json!({
                            "service_provider": reward.service_provider_id,
                            "amount": reward.amount,
                        }))?,
                        MobileReward::UnallocatedReward(reward) => print_json(&json!({
                            "unallocated_reward_type": reward.reward_type,
                            "amount": reward.amount,
                        }))?,
                        // MobileReward::RadioRewardV2(radio_reward_v2) => todo!(),
                        // MobileReward::PromotionReward(promotion_reward) => todo!(),
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
                    let report = RewardManifest::decode(msg)?;
                    print_json(&report)?;
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
                        "timestamp": packet_report.gateway_tmst
                    }))?;
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
                FileType::OracleBoostingReport => {
                    #[derive(serde::Serialize)]
                    enum Assignment {
                        A,
                        B,
                        C,
                    }

                    #[derive(serde::Serialize)]
                    struct OracleBoostingHexAssignment {
                        location: String,
                        assignment_multiplier: u32,
                        urbanized: Assignment,
                    }

                    let report = OracleBoostingReportV1::decode(msg)?;
                    let assignments: Vec<_> = report
                        .assignments
                        .into_iter()
                        .map(|assignment| OracleBoostingHexAssignment {
                            location: assignment.location,
                            assignment_multiplier: assignment.assignment_multiplier,
                            urbanized: match assignment.urbanized {
                                0 => Assignment::A,
                                1 => Assignment::B,
                                _ => Assignment::C,
                            },
                        })
                        .collect();

                    print_json(&json!({
                        "coverage_object": uuid::Uuid::from_slice(report.coverage_object.as_slice()).unwrap(),
                        "assignments": assignments,
                        "timestamp": report.timestamp.to_timestamp()?,
                    }))?
                }
                FileType::CoverageObject => {
                    let coverage = CoverageObjectV1::decode(msg)?;
                    let coverage = CoverageObject::try_from(coverage.coverage_object.unwrap())?;
                    print_json(&json!({
                        "pub_key": coverage.pub_key,
                        "uuid": coverage.uuid,
                        "coverage_claim_time": coverage.coverage_claim_time,
                        "coverage": coverage.coverage,
                    }))?;
                }
                FileType::UniqueConnectionsReport => {
                    let report = UniqueConnectionsIngestReportV1::decode(msg)?;
                    let req = UniqueConnectionReq::try_from(report.report.unwrap())?;
                    print_json(&json!({
                        "pubkey": req.pubkey,
                        "start_timestamp": req.start_timestamp,
                        "end_timestamp": req.end_timestamp,
                        "unique_connections": req.unique_connections,
                        "timestamp": req.timestamp,
                        "carrier_key": req.carrier_key,
                    }))?;
                }
                FileType::VerifiedUniqueConnectionsReport => {
                    let verified_report = VerifiedUniqueConnectionsIngestReportV1::decode(msg)?;
                    let report = verified_report.report.unwrap();
                    let req = UniqueConnectionReq::try_from(report.report.unwrap())?;
                    print_json(&json!({
                        "pubkey": req.pubkey,
                        "start_timestamp": req.start_timestamp,
                        "end_timestamp": req.end_timestamp,
                        "unique_connections": req.unique_connections,
                        "timestamp": req.timestamp,
                        "carrier_key": req.carrier_key,
                    }))?;
                }
                FileType::EntityOwnershipChangeReport => {
                    let report = EntityOwnershipChangeReportV1::decode(msg)?;
                    let report = report.report.unwrap();
                    let change = report.change.unwrap();
                    print_json(&json!({
                        "entity_pubkey": PublicKeyBinary::from(change.entity_pub_key.unwrap().value),
                        "timestamp": change.timestamp_seconds,
                        "asset": bs58::encode(change.asset.unwrap().value).into_string(),
                        "block": change.block,
                        "owner": bs58::encode(change.owner.clone().unwrap().wallet.unwrap().value).into_string(),
                        "owner_type": change.owner.unwrap().r#type(),
                    }))?;
                }
                FileType::EntityRewardDestinationChangeReport => {
                    let report = EntityRewardDestinationChangeReportV1::decode(msg)?;
                    let report = report.report.unwrap();
                    let change = report.change.unwrap();
                    print_json(&json!({
                        "entity_pubkey": PublicKeyBinary::from(change.entity_pub_key.unwrap().value),
                        "timestamp": change.timestamp_seconds,
                        "asset": bs58::encode(change.asset.unwrap().value).into_string(),
                        "block": change.block,
                        "rewards_destination": match change.rewards_destination.unwrap() {
                            RewardsDestination::RewardsRecipient(recipient) => json!({
                                "recipient": bs58::encode(recipient.value).into_string(),
                            }),
                            RewardsDestination::RewardsSplitV1(split) => json!({
                                "pubkey": bs58::encode(split.pub_key.unwrap().value).into_string(),
                                "total_shares": split.total_shares,
                                "recipients": split.recipients.iter().map(|r| {
                                    json!({
                                        "pubkey": bs58::encode(r.recipient.clone().unwrap().value).into_string(),
                                        "reward_amount": match r.reward_amount.unwrap() {
                                            RewardAmount::FixedAmount(amount) => json!({
                                                "type": "fixed_amount",
                                                "amount": amount,
                                            }),
                                            RewardAmount::Shares(shares) => json!({
                                                "type": "shares",
                                                "shares": shares,
                                            }),
                                        },
                                        "authority": r.authority.clone().map(|a| bs58::encode(a.value).into_string()),
                                    })
                                }).collect::<Vec<_>>(),
                            }),
                        },
                    }))?;
                }
                missing_filetype => println!("No dump for {missing_filetype}"),
            }
        }

        Ok(())
    }
}
