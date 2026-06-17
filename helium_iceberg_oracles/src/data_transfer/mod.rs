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

pub mod burned_session;
pub mod invalid_session;
pub mod session;

pub use burned_session::IcebergBurnedDataTransferSession;
pub use invalid_session::IcebergInvalidDataTransferSession;
pub use session::IcebergDataTransferSession;

pub const NAMESPACE: &str = "data_transfer";

/// Column appended to the `invalid_sessions` table, recording why a session was
/// rejected (a `ReportStatus` string name).
pub const REASON_COLUMN: &str = "reason";
