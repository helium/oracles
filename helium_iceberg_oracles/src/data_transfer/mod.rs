//! Iceberg tables in the `data_transfer` namespace.
//!
//! Written by `mobile-packet-verifier` (the burn pipeline) and read by
//! `mobile-verifier` (the reward pipeline). Keeping the definitions here means
//! the writer and readers share one source of truth and cannot drift.
//!
//! - `sessions` — valid data transfer sessions (the ingest report).
//! - `invalid_sessions` — rejected sessions (same schema plus a `reason` column).
//! - `burned_sessions` — sessions whose DC has been burned; the input to mobile
//!   data-transfer rewards.
//! - `multiplier_ticket_history` — every HIP-150 multiplier ticket seen,
//!   accepted or refused. Append-only.
//! - `multiplier_ticket_inventory` — the multiplier currently in force per
//!   hotspot, merged from the history on a schedule. Follows the pattern
//!   `network-dbt` uses for `enabled_carriers_inventory` over
//!   `enabled_carriers_history`, with our own job issuing the SQL.

pub mod burned_session;
pub mod invalid_session;
pub mod multiplier_ticket_history;
pub mod multiplier_ticket_inventory;
pub mod session;

pub use burned_session::IcebergBurnedDataTransferSession;
pub use invalid_session::IcebergInvalidDataTransferSession;
pub use multiplier_ticket_history::IcebergMultiplierTicket;
pub use multiplier_ticket_inventory::IcebergMultiplierInventory;
pub use session::IcebergDataTransferSession;

pub const NAMESPACE: &str = "data_transfer";

/// Column appended to the `invalid_sessions` table, recording why a session was
/// rejected (a `ReportStatus` string name).
pub const REASON_COLUMN: &str = "reason";
