use crate::rewarder;
use chrono::{DateTime, Utc};
use mobile_config::EpochInfo;
use sqlx::{Pool, Postgres};

const LAST_REWARDED_END_TIME: &str = "last_rewarded_end_time";
const DATA_TRANSFER_REWARDS_SCALE: &str = "data_transfer_rewards_scale";
const POC_REWARDED_RADIOS: &str = "poc_rewarded_radios";
const DATA_TRANSFER_REWARDED_GATEWAYS: &str = "data_transfer_rewarded_gateways";
const MAPPERS_REWARDED: &str = "mappers_rewarded";
const DATA_SESSION_DC_DIVERGENCE: &str = "data_session_dc_divergence";
const DATA_SESSION_HOTSPOT_DIVERGENCE: &str = "data_session_hotspot_divergence";

pub async fn initialize(db: &Pool<Postgres>) -> anyhow::Result<()> {
    let next_reward_epoch = rewarder::next_reward_epoch(db).await?;
    let epoch_period: EpochInfo = next_reward_epoch.into();
    last_rewarded_end_time(epoch_period.period.start);
    Ok(())
}

pub fn last_rewarded_end_time(timestamp: DateTime<Utc>) {
    metrics::gauge!(LAST_REWARDED_END_TIME).set(timestamp.timestamp() as f64);
}

pub fn data_transfer_rewards_scale(scale: f64) {
    metrics::gauge!(DATA_TRANSFER_REWARDS_SCALE).set(scale);
}

pub fn poc_rewarded_radios(count: u64) {
    metrics::gauge!(POC_REWARDED_RADIOS).set(count as f64);
}

pub fn data_transfer_rewarded_gateways(count: u64) {
    metrics::gauge!(DATA_TRANSFER_REWARDED_GATEWAYS).set(count as f64);
}

pub fn mappers_rewarded(count: u64) {
    metrics::gauge!(MAPPERS_REWARDED).set(count as f64);
}

/// Signed delta (trino - postgres) in total rewardable DC for the epoch.
/// Emitted in `Compare` mode to validate the Trino data-session path.
pub fn data_session_dc_divergence(delta: i64) {
    metrics::gauge!(DATA_SESSION_DC_DIVERGENCE).set(delta as f64);
}

/// Number of hotspots that differ between the Postgres and Trino data-session
/// aggregates (mismatched totals or present in only one source).
pub fn data_session_hotspot_divergence(count: u64) {
    metrics::gauge!(DATA_SESSION_HOTSPOT_DIVERGENCE).set(count as f64);
}
