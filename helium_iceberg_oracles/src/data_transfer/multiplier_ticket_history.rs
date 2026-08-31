//! `data_transfer.multiplier_ticket_history` — every HIP-150 ticket ever seen.
//!
//! Append-only, one row per ticket received, accepted or not. HIP-150 requires
//! every multiplier in force to be externally auditable, and recording refusals
//! too means the record answers "why is this hotspot not multiplied" as well as
//! "why is it".
//!
//! This is the audit record. What the burn actually charges against is the
//! `data_transfer_multipliers` table in mobile-packet-verifier's Postgres.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use file_store_oracles::mobile::data_transfer_multiplier::{
    ticket_status_string, VerifiedDataTransferMultiplierTicketReport,
};

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
    /// `invalid_multiplier`. A value that merely falls outside HIP-150's 1-to-5
    /// range but still fits the column *is* stored, so the record shows what
    /// was asked for.
    pub multiplier: Option<MultiplierDecimal>,
    pub signer: String,
    pub message: String,
    /// The verdict: `valid`, `invalid_signer`, `invalid_multiplier`,
    /// `invalid_hotspot_key` or `invalid_timestamp`. Rejected tickets are kept.
    /// See `ticket_status_string` for why these are not the proto enum's own
    /// names.
    pub status: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_timestamptz("signed_timestamp"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("verified_timestamp"),
            // Optional because a refused ticket may carry no usable multiplier
            // at all, and the row still has to record the refusal. A required
            // column rejects the write instead — arrow refuses a null in a
            // non-nullable field at flush — which fails the file's transaction
            // and leaves the poller retrying it forever.
            FieldDefinition::optional_decimal("multiplier", MULTIPLIER_PRECISION, MULTIPLIER_SCALE),
            FieldDefinition::required_string("signer"),
            FieldDefinition::required_string("message"),
            FieldDefinition::required_string("status"),
        ])
        // Bucketed on the hotspot, because that is how this table gets read:
        // "what is this hotspot on now", or "what has it ever been granted".
        // Both want every row for one key, which a date partition cannot prune.
        //
        // Four buckets, not more. The eligible population is small -- HIP-150
        // puts candidate venues at 2.7% of earning locations, and enrollment
        // needs an agreement and custodial ownership on top of that -- and each
        // one gets a handful of tickets. Splitting a table this size further
        // buys no pruning worth measuring and costs a file per bucket on every
        // write.
        //
        // The count can be raised later. Iceberg keeps existing files on their
        // old spec, reads span both, and `EXECUTE optimize` handles the mix.
        .with_partition(PartitionDefinition::bucket(
            "hotspot_pubkey",
            "hotspot_pubkey_bucket",
            4,
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
            status: ticket_status_string(verified.status).to_string(),
        }
    }
}
