//! Iceberg tables in the `rewards` namespace.
//!
//! Written by `mobile-verifier` (the rewarder) and read by `reward-index`.
//! Each table receives one idempotent append per reward epoch.
//!
//! - `data_transfer` — per-hotspot data transfer rewards.
//! - `service_provider` — service provider rewards.
//! - `unallocated` — reward pool left undistributed (e.g. rounding dust).

pub mod data_transfer;
pub mod service_provider;
pub mod unallocated;

pub use data_transfer::IcebergGatewayReward;
pub use service_provider::IcebergServiceProviderReward;
pub use unallocated::IcebergUnallocatedReward;

pub const NAMESPACE: &str = "rewards";
