//! Shared Iceberg table schemas for Helium Oracles.
//!
//! Generic Iceberg machinery (catalog, writers, test harness) lives in
//! `helium-iceberg`; this crate holds the Helium-specific table definitions and
//! row types shared across oracle services.
//!
//! A schema belongs here once more than one crate needs it — e.g. the
//! `data_transfer` tables are written by `mobile-packet-verifier` and read by
//! `mobile-verifier`, and the `rewards` tables are written by `mobile-verifier`
//! and read by `reward-index`. Single-owner tables stay in their owning crate.

pub mod data_transfer;
pub mod decimal;
pub mod rewards;

pub use decimal::IcebergDecimal;
