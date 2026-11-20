//! `icicle` routes chain change reports from the ingest chain bucket into
//! Iceberg, using the `glacon` route runtime.
//!
//! One route today, `mobile_hotspot_history`: every
//! `mobile_hotspot_change_report` the ingest service writes becomes one row
//! of `chain.mobile_hotspot_history`, with the asserted H3 cell's center added
//! as `asserted_lat` and `asserted_lon`. See the crate README for the data
//! flow.

extern crate tls_init;

pub mod cli;
pub mod data;
pub mod metrics;
pub mod settings;

pub use cli::Cli;
pub use settings::Settings;

#[cfg(test)]
tls_init::include_tls_tests!();
