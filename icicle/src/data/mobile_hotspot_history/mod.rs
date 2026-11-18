//! The mobile hotspot history route: `mobile_hotspot_change_report` files
//! from the ingest chain bucket become rows of `chain.mobile_hotspot_history`.
//!
//! ```text
//! FileStoreInput ──MobileHotspotChangeReport──▶ MobileHotspotHistoryMiddleware ──MobileHotspotHistory──▶ IcebergOutput
//!  (input.rs)                                    (middleware.rs)                                          (output.rs)
//! ```
//!
//! - [`input::MobileHotspotChangeInput`] makes no decisions: each message is
//!   handed on untouched, with its file time and position.
//! - [`middleware::MobileHotspotHistoryMiddleware`] makes all of them: it
//!   rejects a report it can't use, converts the rest field for field as
//!   `glacon-rs` did for `network.chain.mobile_hotspot_history`, and adds the
//!   center of the asserted H3 cell as `asserted_lat` and `asserted_lon`.
//! - [`output::MobileHotspotHistoryTable`] writes one row per accepted report.
//!
//! Polling, batching, the ledger, replay and supervision all belong to glacon.

pub mod input;
pub mod middleware;
pub mod output;
pub mod settings;

pub use input::MobileHotspotChangeInput;
pub use middleware::{MobileHotspotHistoryMiddleware, Rejected};
pub use output::MobileHotspotHistoryTable;
pub use settings::Settings;

use chrono::{DateTime, FixedOffset};
use glacon_helium::{FileStoreInput, IcebergOutput, Result, Route};
use helium_proto::services::chain_rewardable_entities::MobileHotspotChangeReportV1;
use serde::Serialize;

/// Name of the route. Appears in every metric and names the ledger's
/// directory, so it is as good as a schema version: do not change it.
pub const ROUTE: &str = "mobile_hotspot_history";

/// Name of the single Iceberg output.
pub const OUTPUT: &str = "mobile_hotspot_history";

/// Prefix of the files the ingest service writes, without the trailing dot.
pub const FILE_PREFIX: &str = "mobile_hotspot_change_report";

/// The input name this service is deployed with. It is the ledger's primary
/// key: renaming it orphans the ledger's rows and replays the whole bucket
/// from `start_after`. Pinned, and tested, for that reason.
pub const INPUT: &str = "chain";

pub const DEFAULT_NAMESPACE: &str = "chain";
pub const DEFAULT_TABLE_NAME: &str = "mobile_hotspot_history";

pub type MobileHotspotHistoryRoute =
    Route<MobileHotspotChangeReport, MobileHotspotHistory, Rejected>;

// -------------------------------------------------------------- the records

/// One message of a `mobile_hotspot_change_report` file, as read: the report
/// untouched, plus where it came from. What the input produces and the
/// middleware consumes.
#[derive(Clone, Debug, PartialEq)]
pub struct MobileHotspotChangeReport {
    /// Epoch-**milliseconds** encoded in the source file's name.
    pub file_ts: u64,
    /// Position of this message within its file, starting at 0.
    pub record_index: u64,
    pub report: MobileHotspotChangeReportV1,
}

/// One row of `chain.mobile_hotspot_history`: a converted report, plus the
/// center of its asserted cell. What the middleware produces and the output
/// writes.
///
/// Serialized to Arrow by name, so field names are the table's columns.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MobileHotspotHistory {
    /// Epoch-**milliseconds** encoded in the source file's name.
    pub file_ts: u64,
    /// Position of the report within its file, starting at 0.
    pub record_index: u64,
    /// When ingest received the change.
    pub received_timestamp: DateTime<FixedOffset>,
    pub block: u64,
    /// When the change took effect on chain.
    pub timestamp: DateTime<FixedOffset>,
    pub pub_key: String,
    pub animal_name: Option<String>,
    pub asset: String,
    pub serial_number: Option<String>,
    /// `CBRS`, `WIFI_INDOOR`, `WIFI_OUTDOOR`, `WIFI_DATA_ONLY` or `UNKNOWN`.
    pub device_type: String,
    /// The asserted H3 cell as sent; `None` when the hotspot is unasserted.
    pub asserted_hex: Option<String>,
    /// Latitude of the center of `asserted_hex`, in degrees. `None` when there
    /// is no asserted cell or it is not a valid H3 cell.
    pub asserted_lat: Option<f64>,
    /// Longitude of the center of `asserted_hex`, in degrees. `None` when
    /// there is no asserted cell or it is not a valid H3 cell.
    pub asserted_lon: Option<f64>,
    pub azimuth: u32,
    pub signer: String,
}

// ---------------------------------------------------------------- the route

/// Builds the route: the chain bucket -> [`MobileHotspotHistoryMiddleware`]
/// -> the table.
pub fn route(settings: &Settings) -> Result<MobileHotspotHistoryRoute> {
    let mut builder = Route::builder(ROUTE, settings.route.clone());

    for (name, input) in &settings.inputs {
        builder = builder.input(
            name,
            FileStoreInput::new(MobileHotspotChangeInput, input.clone()),
        );
    }

    let table = MobileHotspotHistoryTable::from(&settings.table);

    builder
        .fresh_state(settings.fresh_state.into())
        .middleware(MobileHotspotHistoryMiddleware)
        .output(OUTPUT, IcebergOutput::new(table, settings.output.clone()))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `build()` does no I/O -- it validates that there is at least one input
    /// and one output and that every name is usable -- so the shipped
    /// configuration can be proved to produce a working route without any
    /// services.
    #[test]
    fn the_shipped_settings_build_a_route() {
        let template = concat!(env!("CARGO_MANIFEST_DIR"), "/pkg/settings-template.toml");
        let settings = crate::Settings::new(Some(template)).expect("template must load");
        let history = settings.mobile_hotspot_history.expect("route configured");

        route(&history).expect("the shipped settings must build a route");
    }

    /// The route name keys glacon's metrics and ledger directory, and the
    /// input name keys the ledger's rows: renaming either replays the whole
    /// bucket, so changing this test should be a deliberate act.
    #[test]
    fn the_names_are_pinned() {
        assert_eq!(ROUTE, "mobile_hotspot_history");
        assert_eq!(INPUT, "chain");
        assert_eq!(FILE_PREFIX, "mobile_hotspot_change_report");
        assert_eq!(DEFAULT_NAMESPACE, "chain");
        assert_eq!(DEFAULT_TABLE_NAME, "mobile_hotspot_history");
    }
}
