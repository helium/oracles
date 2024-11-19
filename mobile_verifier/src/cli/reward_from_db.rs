use crate::{
    heartbeats::HeartbeatReward,
    reward_shares::{
        get_scheduled_tokens_for_poc, CoverageShares, DataTransferAndPocAllocatedRewardBuckets,
    },
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    sp_boosted_rewards_bans::BannedRadios,
    speedtests_average::SpeedtestAverages,
    unique_connections, Settings, MOBILE_SUB_DAO_ONCHAIN_ADDRESS,
};
use anyhow::Result;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::poc_mobile as proto;
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{sub_dao_client::SubDaoEpochRewardInfoResolver, SubDaoClient},
};
use serde_json::json;
use std::{collections::HashMap, str::FromStr};

/// Reward an epoch from the entries in the database
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    reward_epoch: u64,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        // TODO: do we want to continue maintaining this cli ?

        let reward_epoch = self.reward_epoch;

        let sub_dao_pubkey = PublicKeyBinary::from_str(&MOBILE_SUB_DAO_ONCHAIN_ADDRESS)?;
        let sub_dao_rewards_client = SubDaoClient::from_settings(&settings.config_client)?;
        let reward_info = sub_dao_rewards_client
            .resolve_info(&sub_dao_pubkey, reward_epoch)
            .await?
            .ok_or(anyhow::anyhow!(
                "No reward info found for epoch {}",
                reward_epoch
            ))?;

        tracing::info!(
            "Rewarding shares from the following time range: {} to {}",
            reward_info.epoch_period.start,
            reward_info.epoch_period.end
        );
        let expected_rewards = get_scheduled_tokens_for_poc(reward_info.epoch_emissions);

        let (shutdown_trigger, _shutdown_listener) = triggered::trigger();
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;

        let heartbeats = HeartbeatReward::validated(&pool, &reward_info.epoch_period);
        let speedtest_averages =
            SpeedtestAverages::aggregate_epoch_averages(reward_info.epoch_period.end, &pool)
                .await?;

        let unique_connections = unique_connections::db::get(&pool, &epoch).await?;

        let reward_shares = CoverageShares::new(
            &pool,
            heartbeats,
            &speedtest_averages,
            &BoostedHexes::default(),
            &BoostedHexEligibility::default(),
            &BannedRadios::default(),
            &unique_connections,
            &reward_info.epoch_period,
        )
        .await?;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        let radio_rewards = reward_shares
            .into_rewards(
                DataTransferAndPocAllocatedRewardBuckets::new(reward_info.epoch_emissions),
                &reward_info,
            )
            .ok_or(anyhow::anyhow!("no rewardable events"))?
            .1;
        for (_reward_amount, reward, _v2) in radio_rewards {
            if let Some(proto::mobile_reward_share::Reward::RadioReward(proto::RadioReward {
                hotspot_key,
                poc_reward,
                ..
            })) = reward.reward
            {
                total_rewards += poc_reward;
                *owner_rewards
                    .entry(PublicKey::try_from(hotspot_key)?)
                    .or_default() += poc_reward;
            }
        }
        let rewards: Vec<_> = owner_rewards.into_iter().collect();
        let mut multiplier_count = HashMap::<_, usize>::new();
        let speedtest_multipliers: Vec<_> = speedtest_averages
            .averages
            .into_iter()
            .map(|(pub_key, average)| {
                let reward_multiplier = average.reward_multiplier;
                *multiplier_count.entry(reward_multiplier).or_default() += 1;
                (pub_key, reward_multiplier)
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "multiplier_count": multiplier_count,
                "speedtest_multipliers": speedtest_multipliers,
                "rewards": rewards,
                "total_rewards": total_rewards,
                "expected_rewards": expected_rewards,
            }))?
        );

        shutdown_trigger.trigger();
        Ok(())
    }
}
