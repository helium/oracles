use crate::{
    heartbeats::HeartbeatReward,
    reward_shares::{get_scheduled_tokens_for_poc_and_dc, CoveragePoints},
    speedtests_average::SpeedtestAverages,
    Settings,
};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile as proto;
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::HashMap;

/// Reward a period from the entries in the database
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: NaiveDateTime,
    #[clap(long)]
    end: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let Self { start, end } = self;

        let start = DateTime::from_utc(start, Utc);
        let end = DateTime::from_utc(end, Utc);

        tracing::info!("Rewarding shares from the following time range: {start} to {end}");
        let epoch = start..end;
        let expected_rewards = get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start);

        let (shutdown_trigger, _shutdown_listener) = triggered::trigger();
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;

        let heartbeats =
            HeartbeatReward::validated(&pool, &epoch, settings.max_asserted_distance_deviation);
        let speedtest_averages =
            SpeedtestAverages::aggregate_epoch_averages(epoch.end, &pool).await?;
        let reward_shares =
            CoveragePoints::aggregate_points(&pool, heartbeats, &speedtest_averages, end).await?;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        let radio_rewards = reward_shares
            .into_rewards(Decimal::ZERO, &epoch)
            .ok_or(anyhow::anyhow!("no rewardable events"))?;
        for reward in radio_rewards {
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
