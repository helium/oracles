use std::ops::Range;

use chrono::{DateTime, Utc};
use helium_proto::ServiceProviderAllocation;
pub use promotions::daemon::PromotionDaemon;
pub use reward::ServiceProviderRewardInfos;
use sqlx::PgPool;

pub mod dc_sessions;
pub mod promotions;
pub mod reward;

pub mod db {
    pub use super::dc_sessions::fetch_dc_sessions;
    pub use super::promotions::funds::fetch_promotion_funds;
    pub use super::promotions::rewards::{clear_promotion_rewards, fetch_promotion_rewards};
}

// This type is used in lieu of the helium_proto::ServiceProvider enum so we can
// handle more than a single value without adding a hard deploy dependency to
// mobile-verifier when a new carrier is added..
pub type ServiceProviderId = i32;

pub fn get_scheduled_tokens(reward_period: &Range<DateTime<Utc>>) -> rust_decimal::Decimal {
    let duration = reward_period.end - reward_period.start;
    crate::reward_shares::get_scheduled_tokens_for_service_providers(duration)
}

pub async fn reward_data_sp_allocations(
    pool: &PgPool,
) -> anyhow::Result<Vec<ServiceProviderAllocation>> {
    let funds = db::fetch_promotion_funds(pool).await?;
    let mut sp_allocations = vec![];

    for (sp_id, bps) in funds.0.into_iter() {
        sp_allocations.push(ServiceProviderAllocation {
            service_provider: sp_id,
            incentive_escrow_fund_bps: bps as u32,
        });
    }

    Ok(sp_allocations)
}
