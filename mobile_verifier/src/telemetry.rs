use crate::rewarder;
use chrono::{DateTime, Utc};
use mobile_config::EpochPeriod;
use sqlx::{Pool, Postgres};

const LAST_REWARDED_END_TIME: &str = "last_rewarded_end_time";
const DATA_TRANSFER_REWARDS_SCALE: &str = "data_transfer_rewards_scale";

pub async fn initialize(db: &Pool<Postgres>) -> anyhow::Result<()> {
    let next_reward_epoch = rewarder::next_reward_epoch(db).await?;
    let epoch_period: EpochPeriod = next_reward_epoch.try_into()?;
    last_rewarded_end_time(epoch_period.period.start);
    Ok(())
}

pub fn last_rewarded_end_time(timestamp: DateTime<Utc>) {
    metrics::gauge!(LAST_REWARDED_END_TIME).set(timestamp.timestamp() as f64);
}

pub fn data_transfer_rewards_scale(scale: f64) {
    metrics::gauge!(DATA_TRANSFER_REWARDS_SCALE).set(scale);
}
