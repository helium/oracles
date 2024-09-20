pub use daemon::SpPromotionDaemon;
pub use rewards::clear_promotion_rewards;

pub mod daemon;
pub mod funds;
pub mod rewards;

// This type is used in lieu of the helium_proto::ServiceProvider enum so we can
// handle more than a single value without adding a hard deploy dependency to
// mobile-verifier when a new carrier is added..
pub type ServiceProviderId = i32;
