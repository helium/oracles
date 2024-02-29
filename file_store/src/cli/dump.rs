use crate::{
    cli::print_json,
    file_source,
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    iot_packet::IotValidPacket,
    mobile_session::{DataTransferSessionIngestReport, InvalidDataTransferIngestReport},
    mobile_subscriber::{SubscriberLocationIngestReport, VerifiedSubscriberLocationIngestReport},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::MsgDecode,
    wifi_heartbeat::WifiHeartbeatIngestReport,
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
    BlockchainTxn, BoostedHexUpdateV1 as BoostedHexUpdateProto, Message, PriceReportV1,
    RewardManifest, SubnetworkRewards,
};
use serde_json::json;
use serde::Serialize;
use std::io;
use std::collections::HashMap;
use std::path::PathBuf;

/// Print information about a given store file.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    mainnet: PathBuf,
    labnet: PathBuf,
}

#[derive(Default, Serialize)]
pub struct Rewards {
    mainnet: u64,
    labnet: u64
}

#[derive(Serialize)]
pub struct Avg {
    avg_diff: i128,
    not_rewarded_mainnet: usize,
    not_rewarded_labnet: usize,
    total_hotspots_rewarded: usize,
}

impl Cmd {
    pub async fn run(&self, _settings: &Settings) -> Result {
	let mut rewards = HashMap::<_, Rewards>::new();

        let mut file_stream = file_source::source([&self.mainnet]);
        while let Some(result) = file_stream.next().await {
            let msg = result?;
	    let reward = MobileRewardShare::decode(msg)?;
            match reward.reward {
                Some(Reward::RadioReward(reward)) => {
		    let hotspot_key = PublicKey::try_from(reward.hotspot_key)?;
		    let poc_reward = reward.poc_reward;
		    rewards.entry(hotspot_key)
			.or_default().mainnet += poc_reward;
		},
                _ => (),
	    }
	}

	let mut file_stream = file_source::source([&self.labnet]);
        while let Some(result) = file_stream.next().await {
            let msg = result?;
	    let reward = MobileRewardShare::decode(msg)?;
            match reward.reward {
                Some(Reward::RadioReward(reward)) => {
		    let hotspot_key = PublicKey::try_from(reward.hotspot_key)?;
		    let poc_reward = reward.poc_reward;
		    rewards.entry(hotspot_key)
			.or_default().labnet += poc_reward;
		},
                _ => (),
	    }
	}

	let mut total_diff = 0_i128;
	let mut not_rewarded_mainnet = 0;
	let mut not_rewarded_labnet = 0;

	for (_, rewards) in &rewards {
	    total_diff += rewards.labnet as i128 - rewards.mainnet as i128;
	    if rewards.labnet == 0 {
		not_rewarded_labnet += 1;
	    }
	    if rewards.mainnet == 0 {
		not_rewarded_mainnet += 1;
	    }
	}

	let avg = Avg {
	    avg_diff: total_diff / rewards.len() as i128,
	    not_rewarded_mainnet,
	    not_rewarded_labnet,
	    total_hotspots_rewarded: rewards.len(),
	};

	print_json(&avg)?;

	Ok(())
    }
}
