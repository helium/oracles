use chrono::{Duration, Utc};
pub use dc_sessions::{get_dc_sessions, ServiceProviderDCSessions};
use helium_crypto::PublicKeyBinary;
use mobile_config::sub_dao_epoch_reward_info::ResolvedSubDaoEpochRewardInfo;
pub use promotions::{get_promotions, ServiceProviderPromotions};
pub use reward::ServiceProviderRewardInfos;
use rust_decimal::Decimal;
use std::str::FromStr;

mod dc_sessions;
mod promotions;
mod reward;

pub const EPOCH_ADDRESS: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
pub const SUB_DAO_ADDRESS: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

// This type is used in lieu of the helium_proto::ServiceProvider enum so we can
// handle more than a single value without adding a hard deploy dependency to
// mobile-verifier when a new carrier is added..
pub type ServiceProviderId = i32;

pub fn get_scheduled_tokens(total_emission_pool: Decimal) -> rust_decimal::Decimal {
    crate::reward_shares::get_scheduled_tokens_for_service_providers(total_emission_pool)
}

pub fn default_rewards_info(
    total_emissions: u64,
    epoch_duration: Duration,
) -> ResolvedSubDaoEpochRewardInfo {
    let now = Utc::now();
    ResolvedSubDaoEpochRewardInfo {
        epoch: 1,
        epoch_address: PublicKeyBinary::from_str(EPOCH_ADDRESS).unwrap(),
        sub_dao_address: PublicKeyBinary::from_str(SUB_DAO_ADDRESS).unwrap(),
        epoch_period: (now - epoch_duration)..now,
        epoch_emissions: Decimal::from(total_emissions),
        rewards_issued_at: now,
    }
}
