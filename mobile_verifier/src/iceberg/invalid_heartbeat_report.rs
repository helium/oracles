//! `hotspots.invalid_heartbeat_reports`: rejected heartbeats, as reported.
//!
//! Same schema as [`super::heartbeat_report`] (`hotspots.heartbeat_reports`)
//! plus a `reason` column recording the `HeartbeatValidity` that rejected the
//! row, kept for auditability. The table definition is derived from the valid
//! one so the two never drift, and rows are built from an already-converted
//! [`IcebergHeartbeatReport`] so the field mapping is shared.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::HeartbeatValidity;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{heartbeat_report::IcebergHeartbeatReport, NAMESPACE, REASON_COLUMN};

pub const TABLE_NAME: &str = "invalid_heartbeat_reports";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidHeartbeatReport {
    pub hotspot_pubkey: String,
    pub received_timestamp: DateTime<FixedOffset>,
    pub heartbeat_timestamp: DateTime<FixedOffset>,
    pub lat: f64,
    pub lon: f64,
    pub centered_lat: Option<f64>,
    pub centered_lon: Option<f64>,
    pub location_validation_timestamp: Option<DateTime<FixedOffset>>,
    pub location_source: String,
    pub reason: String,
}

impl IcebergInvalidHeartbeatReport {
    /// Tag an already-converted report with the validity that rejected it.
    pub fn new(report: IcebergHeartbeatReport, validity: HeartbeatValidity) -> Self {
        Self {
            hotspot_pubkey: report.hotspot_pubkey,
            received_timestamp: report.received_timestamp,
            heartbeat_timestamp: report.heartbeat_timestamp,
            lat: report.lat,
            lon: report.lon,
            centered_lat: report.centered_lat,
            centered_lon: report.centered_lon,
            location_validation_timestamp: report.location_validation_timestamp,
            location_source: report.location_source,
            reason: super::reason_without_prefix(validity.as_str_name(), "heartbeat_validity_"),
        }
    }
}

/// Reuses the `hotspots.heartbeat_reports` definition, renamed and with a
/// `reason` column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::heartbeat_report::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergInvalidHeartbeatReport>> {
    let all = match trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all.into_vec(),
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}
