use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

use crate::rewarder;

const LAST_REWARDED_END_TIME: &str = "last_rewarded_end_time";
const DATA_TRANSFER_REWARDS_SCALE: &str = "data_transfer_rewards_scale";

pub async fn initialize(db: &Pool<Postgres>) -> anyhow::Result<()> {
    last_rewarded_end_time(rewarder::last_rewarded_end_time(db).await?);

    Ok(())
}

pub fn last_rewarded_end_time(timestamp: DateTime<Utc>) {
    metrics::gauge!(LAST_REWARDED_END_TIME).set(timestamp.timestamp() as f64);
}

pub fn data_transfer_rewards_scale(scale: f64) {
    metrics::gauge!(DATA_TRANSFER_REWARDS_SCALE).set(scale);
}
