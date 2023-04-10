use crate::{
    heartbeats::Heartbeats,
    reward_shares::{get_scheduled_tokens_for_poc_and_dc, PocShares, TransferRewards},
    speedtests::{Average, SpeedtestAverages},
    Settings,
};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use helium_crypto::PublicKey;
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

        let mut follower = settings.follower.connect_follower();
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let (pool, _join_handle) = settings
            .database
            .connect(env!("CARGO_PKG_NAME"), shutdown_listener)
            .await?;

        let heartbeats = Heartbeats::validated(&pool).await?;
        let speedtests = SpeedtestAverages::validated(&pool, epoch.end).await?;
        let reward_shares =
            PocShares::aggregate(&mut follower, heartbeats, speedtests.clone()).await?;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        let transfer_rewards = TransferRewards::empty();
        for (reward, _) in reward_shares.into_rewards(&transfer_rewards, &epoch) {
            total_rewards += reward.amount;
            *owner_rewards
                .entry(PublicKey::try_from(reward.owner_key)?)
                .or_default() += reward.amount;
        }
        let rewards: Vec<_> = owner_rewards.into_iter().collect();
        let mut multiplier_count = HashMap::<_, usize>::new();
        let speedtest_multipliers: Vec<_> = speedtests
            .speedtests
            .into_iter()
            .map(|(pub_key, avg)| {
                let reward_multiplier = Average::from(&avg).reward_multiplier();
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
