use crate::{
    heartbeats::Heartbeats,
    owner_shares::{get_scheduled_tokens, OwnerResolver},
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
        let expected_rewards = get_scheduled_tokens(epoch.start, epoch.end - epoch.start)
            .expect("Couldn't get expected rewards");

        let mut follower = settings.follower.connect_follower()?;
        let pool = settings.database.connect(10).await?;

        let heartbeats = Heartbeats::validated(&pool, epoch.start).await?;
        let speedtests = SpeedtestAverages::validated(&pool, epoch.end).await?;
        let owner_shares = follower
            .owner_shares(heartbeats, speedtests.clone())
            .await?;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        for reward in owner_shares.into_radio_shares(&epoch) {
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

        Ok(())
    }
}
