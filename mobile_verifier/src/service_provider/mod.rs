use std::ops::Range;

use chrono::{DateTime, Utc};

pub use dc_sessions::{get_dc_sessions, ServiceProviderDCSessions};
pub use promotions::{get_promotions, ServiceProviderPromotions};
pub use reward::ServiceProviderRewardInfos;

mod dc_sessions;
mod promotions;
mod reward;

// This type is used in lieu of the helium_proto::ServiceProvider enum so we can
// handle more than a single value without adding a hard deploy dependency to
// mobile-verifier when a new carrier is added..
pub type ServiceProviderId = i32;

pub fn get_scheduled_tokens(reward_period: &Range<DateTime<Utc>>) -> rust_decimal::Decimal {
    let duration = reward_period.end - reward_period.start;
    crate::reward_shares::get_scheduled_tokens_for_service_providers(duration)
}
