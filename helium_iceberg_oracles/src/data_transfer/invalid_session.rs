//! `data_transfer.invalid_sessions` — rejected data transfer sessions.
//!
//! Same schema as [`super::session`] (`data_transfer.sessions`) plus a `reason`
//! column recording the `ReportStatus` that rejected the session. The table
//! definition is derived from the valid one so the two never drift, and rows
//! are built from an already-converted [`IcebergDataTransferSession`] so the
//! field mapping is shared, not duplicated.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use helium_proto::services::poc_mobile::verified_data_transfer_ingest_report_v1::ReportStatus;
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::{session::IcebergDataTransferSession, NAMESPACE, REASON_COLUMN};

pub const TABLE_NAME: &str = "invalid_sessions";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergInvalidDataTransferSession {
    pub report_received_timestamp: DateTime<FixedOffset>,
    pub request_pub_key: String,
    pub rewardable_bytes: u64,
    pub carrier_id: String,
    pub sampling: bool,
    pub data_transfer_event_pub_key: String,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub radio_access_technology: String,
    pub event_id: String,
    pub payer: String,
    pub timestamp: DateTime<FixedOffset>,
    pub reason: String,
}

impl IcebergInvalidDataTransferSession {
    /// Tag an already-converted session with the status that rejected it.
    pub fn new(session: IcebergDataTransferSession, status: ReportStatus) -> Self {
        Self {
            report_received_timestamp: session.report_received_timestamp,
            request_pub_key: session.request_pub_key,
            rewardable_bytes: session.rewardable_bytes,
            carrier_id: session.carrier_id,
            sampling: session.sampling,
            data_transfer_event_pub_key: session.data_transfer_event_pub_key,
            upload_bytes: session.upload_bytes,
            download_bytes: session.download_bytes,
            radio_access_technology: session.radio_access_technology,
            event_id: session.event_id,
            payer: session.payer,
            timestamp: session.timestamp,
            reason: status.as_str_name().to_string(),
        }
    }
}

/// Reuses the `data_transfer.sessions` definition, renamed and with a `reason`
/// column.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    Ok(super::session::table_definition()?
        .with_name(TABLE_NAME)
        .with_field(FieldDefinition::required_string(REASON_COLUMN)))
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergInvalidDataTransferSession>> {
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
