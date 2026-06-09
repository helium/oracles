//! Shared Iceberg table schemas for Helium Oracles.
//!
//! Generic Iceberg machinery (catalog, writers, test harness) lives in
//! `helium-iceberg`; this crate holds the Helium-specific *table definitions* and
//! row types — the schemas shared across oracle services. It is the Iceberg
//! analogue of `file-store-oracles` sitting on top of `file-store`.
//!
//! Tables here are cross-service contracts: a schema belongs in this crate once
//! more than one crate needs it (e.g. `data_transfer.burned_sessions` is written
//! by `mobile-packet-verifier` and read by `mobile-verifier`). Single-owner
//! tables stay in their owning crate.

pub mod data_transfer;
