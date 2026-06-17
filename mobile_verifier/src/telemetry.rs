use crate::rewarder;
use chrono::{DateTime, Utc};
use mobile_config::EpochInfo;
use sqlx::{Pool, Postgres};

const LAST_REWARDED_END_TIME: &str = "last_rewarded_end_time";
const DATA_TRANSFER_REWARDS_SCALE: &str = "data_transfer_rewards_scale";
const POC_REWARDED_RADIOS: &str = "poc_rewarded_radios";
const DATA_TRANSFER_REWARDED_GATEWAYS: &str = "data_transfer_rewarded_gateways";
const MAPPERS_REWARDED: &str = "mappers_rewarded";

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

/// Metrics for the Postgres → Trino data-session migration. Self-contained so
/// the whole module can be deleted once the migration is finished.
pub mod data_session {
    const TRINO_READY: &str = "data_session_trino_ready";
    const MATCHES: &str = "data_session_matches_postgres";
    const DC_DIVERGENCE: &str = "data_session_dc_divergence";
    const BYTES_DIVERGENCE: &str = "data_session_bytes_divergence";
    const HOTSPOT_DIVERGENCE: &str = "data_session_hotspot_divergence";

    /// "When rewards need to run, was the Trino data ready?" — 1.0 when burned
    /// sessions exist past the reward period end, else 0.0. Emitted once per
    /// reward run while a Trino client is configured.
    pub fn trino_ready(ready: bool) {
        metrics::gauge!(TRINO_READY).set(if ready { 1.0 } else { 0.0 });
    }

    /// "When rewards run with Trino data, does it match Postgres exactly?" — 1.0
    /// when the per-hotspot Trino aggregate equals Postgres, else 0.0. The
    /// divergence gauges below quantify how far off it is when this is 0.
    pub fn matches(matches: bool) {
        metrics::gauge!(MATCHES).set(if matches { 1.0 } else { 0.0 });
    }

    /// Signed delta (trino - postgres) in total rewardable DC for the epoch.
    pub fn dc_divergence(delta: i64) {
        metrics::gauge!(DC_DIVERGENCE).set(delta as f64);
    }

    /// Signed delta (trino - postgres) in total rewardable bytes for the epoch.
    pub fn bytes_divergence(delta: i64) {
        metrics::gauge!(BYTES_DIVERGENCE).set(delta as f64);
    }

    /// Number of hotspots that differ between the Postgres and Trino aggregates
    /// (mismatched totals or present in only one source).
    pub fn hotspot_divergence(count: u64) {
        metrics::gauge!(HOTSPOT_DIVERGENCE).set(count as f64);
    }
}
