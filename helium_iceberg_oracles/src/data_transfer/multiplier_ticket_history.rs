//! `data_transfer.multiplier_ticket_history` — every HIP-150 ticket ever seen.
//!
//! Append-only, one row per ticket received, accepted or not. HIP-150 requires
//! every multiplier in force to be externally auditable; recording refusals too
//! means the record answers "why is this hotspot not multiplied" as well as
//! "why is it".
//!
//! This is the log. For "what is current", see
//! [`super::multiplier_ticket_inventory`].

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use file_store_oracles::mobile::data_transfer_multiplier::VerifiedDataTransferMultiplierTicketReport;

use crate::IcebergDecimal;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "multiplier_ticket_history";

/// Precision and scale of the stored multiplier.
///
/// `decimal`, not `double`. Multipliers are negotiated per venue, so they are
/// not always binary-representable — 1.5 and 5 survive a float exactly, 1.3 does
/// not. A public record that has to explain floating-point artifacts is a worse
/// record, and the exact type costs nothing.
pub const MULTIPLIER_PRECISION: u32 = 9;
pub const MULTIPLIER_SCALE: u32 = 6;
// The Iceberg field builder takes u32; the Trino type takes const usize
// generics. Same numbers, spelled for each.
pub const MULTIPLIER_PRECISION_USIZE: usize = MULTIPLIER_PRECISION as usize;
pub const MULTIPLIER_SCALE_USIZE: usize = MULTIPLIER_SCALE as usize;

/// The stored multiplier's type, spelled once.
pub type MultiplierDecimal = IcebergDecimal<MULTIPLIER_PRECISION_USIZE, MULTIPLIER_SCALE_USIZE>;

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergMultiplierTicket {
    pub hotspot_pubkey: String,
    /// When the issuer signed the ticket — what decides which ticket is current.
    pub signed_timestamp: DateTime<FixedOffset>,
    /// When ingest received it.
    pub received_timestamp: DateTime<FixedOffset>,
    /// When this oracle ruled on it.
    pub verified_timestamp: DateTime<FixedOffset>,
    /// `None` when the ticket carried no usable multiplier — absent,
    /// unparseable, or too large for the column. `status` records that it was
    /// refused, but not which of the three it was; all three are
    /// `invalid_multiplier`. A value that is merely out of range *is* stored,
    /// so the record shows what was asked for.
    pub multiplier: Option<MultiplierDecimal>,
    pub signer: String,
    pub message: String,
    /// The verdict, as the proto enum's string name. Rejected tickets are kept.
    pub status: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_timestamptz("signed_timestamp"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("verified_timestamp"),
            FieldDefinition::required_decimal("multiplier", MULTIPLIER_PRECISION, MULTIPLIER_SCALE),
            FieldDefinition::required_string("signer"),
            FieldDefinition::required_string("message"),
            FieldDefinition::required_string("status"),
        ])
        .with_partition(PartitionDefinition::day(
            "received_timestamp",
            "received_timestamp_day",
        ))
        .build()
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergMultiplierTicket>> {
    let all = trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await?
        .into_vec();
    Ok(all)
}

impl From<&VerifiedDataTransferMultiplierTicketReport> for IcebergMultiplierTicket {
    fn from(verified: &VerifiedDataTransferMultiplierTicketReport) -> Self {
        let ticket = &verified.report.report;
        Self {
            hotspot_pubkey: ticket.hotspot_pubkey.to_string(),
            signed_timestamp: ticket.timestamp.into(),
            received_timestamp: verified.report.received_timestamp.into(),
            verified_timestamp: verified.verified_timestamp.into(),
            // A refused ticket may carry a value too large for the column, or
            // none at all; the row still records the refusal.
            multiplier: ticket
                .multiplier
                .and_then(|m| MultiplierDecimal::try_from(m).ok()),
            signer: ticket.signer_pubkey.to_string(),
            message: ticket.message.clone(),
            status: verified.status.as_str_name().to_string(),
        }
    }
}
