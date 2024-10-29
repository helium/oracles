use crate::cli::print_json;
use crate::{file_source, Result, Settings};
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile::mobile_reward_share::Reward::*;
// use helium_proto::services::poc_mobile::promotion_reward::Entity;
use helium_proto::services::poc_mobile::MobileRewardShare;
use prost::Message;
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    path: PathBuf,
}

impl Cmd {
    pub async fn run(&self, _settings: &Settings) -> Result {
        let mut file_stream = file_source::source([&self.path]);

        let mut radio_reward = vec![];
        let mut radio_reward_v2 = vec![];
        let mut gateway_reward = vec![];
        let mut subscriber_reward = vec![];
        let mut service_provider_reward = vec![];
        let mut unallocated_reward = vec![];
        let mut promotion_reward = vec![];

        while let Some(result) = file_stream.next().await {
            let msg = result?;
            let reward = MobileRewardShare::decode(msg)?;
            match reward.reward {
                Some(r) => match r {
                    RadioReward(reward) => radio_reward.push(json!({
                        "hotspot_key":  PublicKey::try_from(reward.hotspot_key)?.to_string(),
                        "cbsd_id": reward.cbsd_id,
                        "poc_reward": reward.poc_reward,
                        "boosted_hexes": reward.boosted_hexes,
                    })),
                    RadioRewardV2(reward) => radio_reward_v2.push(json!({
                        "hotspot_key": PublicKey::try_from(reward.hotspot_key)?.to_string(),
                        "cbsd_id": reward.cbsd_id,
                        "base_poc_reward": reward.base_poc_reward,
                        "boosted_poc_reward" : reward.boosted_poc_reward,
                        "total_poc_reward": reward.base_poc_reward + reward.boosted_poc_reward,
                        "covered_hexes": reward.covered_hexes
                    })),
                    GatewayReward(reward) => gateway_reward.push(json!({
                        "hotspot_key": PublicKey::try_from(reward.hotspot_key)?.to_string(),
                        "dc_transfer_reward": reward.dc_transfer_reward,
                    })),
                    SubscriberReward(reward) => subscriber_reward.push(json!({
                        "subscriber_id": uuid::Uuid::from_slice(&reward.subscriber_id).unwrap(),
                        "discovery_location_amount": reward.discovery_location_amount,
                        "verification_mapping_amount": reward.verification_mapping_amount,
                    })),
                    ServiceProviderReward(reward) => service_provider_reward.push(json!({
                        "service_provider": reward.service_provider_id,
                        "amount": reward.amount,
                    })),
                    UnallocatedReward(reward) => unallocated_reward.push(json!({
                        "unallocated_reward_type": reward.reward_type,
                        "amount": reward.amount,
                    })),
                    PromotionReward(reward) => promotion_reward.push(json!({
                        "entity": reward.entity,
                        "service_provider_amount": reward.service_provider_amount,
                        "matched_amount": reward.matched_amount,
                    })),
                },
                None => todo!(),
            }
        }

        print_json(&json!({
            "radio_reward": radio_reward,
            "radio_reward": radio_reward_v2,
            "gateway_reward": gateway_reward,
            "subscriber_reward": subscriber_reward,
            "service_provider_reward": service_provider_reward,
            "promotion_reward": promotion_reward,
            "unallocated_reward": unallocated_reward,
        }))?;

        Ok(())
    }
}
