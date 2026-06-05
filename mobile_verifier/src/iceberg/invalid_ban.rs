//! `poc.invalid_bans` — rejected ban reports.
//!
//! Same schema as [`super::ban`] (`poc.bans`) plus a `reason` column recording
//! the `VerifiedBanIngestReportStatus` that rejected the row.

use chrono::{DateTime, FixedOffset};
use file_store_oracles::mobile_ban::proto::VerifiedBanIngestReportStatus;
use helium_iceberg::{FieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{ban::IcebergBan, NAMESPACE, REASON_COLUMN};

pub const TABLE_NAME: &str = "invalid_bans";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidBan {
    pub pubkey: String,
    pub ban_pubkey: String,
    pub report_timestamp: DateTime<FixedOffset>,
    pub received_timestamp: DateTime<FixedOffset>,
    pub is_ban: bool,
    pub hotspot_serial: Option<String>,
    pub message: Option<String>,
    pub ban_reason: Option<String>,
    pub ban_type: Option<String>,
    pub expiration_timestamp: Option<DateTime<FixedOffset>>,
    pub reason: String,
}

impl IcebergInvalidBan {
    /// Tag an already-converted ban with the status that rejected it.
    pub fn new(ban: IcebergBan, status: VerifiedBanIngestReportStatus) -> Self {
        Self {
            pubkey: ban.pubkey,
            ban_pubkey: ban.ban_pubkey,
            report_timestamp: ban.report_timestamp,
            received_timestamp: ban.received_timestamp,
            is_ban: ban.is_ban,
            hotspot_serial: ban.hotspot_serial,
            message: ban.message,
            ban_reason: ban.ban_reason,
            ban_type: ban.ban_type,
            expiration_timestamp: ban.expiration_timestamp,
            reason: super::reason_without_prefix(
                status.as_str_name(),
                "verified_ban_ingest_report_status_",
            ),
        }
    }
}

/// Reuses the `poc.bans` definition, renamed and with a `reason` column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::ban::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(trino: &trino_rust_client::Client) -> anyhow::Result<Vec<IcebergInvalidBan>> {
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
