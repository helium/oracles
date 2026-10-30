//! Iceberg tables in the `hotspots` namespace that more than one crate needs.
//!
//! - `bans` — valid ban and unban reports. Written by `mobile-verifier` and
//!   read by `mobile-packet-verifier` to zero out banned hotspots' data
//!   transfer.

pub mod ban;

pub use ban::IcebergBan;

pub const NAMESPACE: &str = "hotspots";
