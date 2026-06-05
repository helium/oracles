//! `poc.invalid_heartbeats` — rejected heartbeats.
//!
//! Same schema as [`super::heartbeat`] (`poc.heartbeats`) plus a `reason`
//! column recording the `HeartbeatValidity` that rejected the row. The table
//! definition is derived from the valid one so the two never drift, and rows
//! are built from an already-converted [`IcebergHeartbeat`] so the field
//! mapping is shared, not duplicated.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::HeartbeatValidity;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{heartbeat::IcebergHeartbeat, NAMESPACE, REASON_COLUMN};

pub const TABLE_NAME: &str = "invalid_heartbeats";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidHeartbeat {
    pub hotspot_pubkey: String,
    pub received_timestamp: DateTime<FixedOffset>,
    pub heartbeat_timestamp: DateTime<FixedOffset>,
    pub device_type: Option<String>,
    pub lat: f64,
    pub lon: f64,
    pub coverage_object: String,
    pub location_validation_timestamp: Option<DateTime<FixedOffset>>,
    pub distance_to_asserted: Option<i64>,
    pub asserted_location: Option<String>,
    pub location_trust_score_multiplier: f64,
    pub location_source: String,
    pub reason: String,
}

impl IcebergInvalidHeartbeat {
    /// Tag an already-converted heartbeat with the validity that rejected it.
    pub fn new(heartbeat: IcebergHeartbeat, validity: HeartbeatValidity) -> Self {
        Self {
            hotspot_pubkey: heartbeat.hotspot_pubkey,
            received_timestamp: heartbeat.received_timestamp,
            heartbeat_timestamp: heartbeat.heartbeat_timestamp,
            device_type: heartbeat.device_type,
            lat: heartbeat.lat,
            lon: heartbeat.lon,
            coverage_object: heartbeat.coverage_object,
            location_validation_timestamp: heartbeat.location_validation_timestamp,
            distance_to_asserted: heartbeat.distance_to_asserted,
            asserted_location: heartbeat.asserted_location,
            location_trust_score_multiplier: heartbeat.location_trust_score_multiplier,
            location_source: heartbeat.location_source,
            reason: super::reason_without_prefix(validity.as_str_name(), "heartbeat_validity_"),
        }
    }
}

/// Reuses the `poc.heartbeats` definition, renamed and with a `reason` column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::heartbeat::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergInvalidHeartbeat>> {
    let all = match trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all.into_vec(),
        Err(trino_rust_client::error::Error::EmptyData) => vec![],
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}
