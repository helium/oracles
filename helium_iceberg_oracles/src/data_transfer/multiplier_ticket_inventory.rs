//! `data_transfer.multiplier_ticket_inventory` — the multiplier currently in
//! force per hotspot.
//!
//! One row per ticketed hotspot, refreshed from
//! [`super::multiplier_ticket_history`] by a periodic `MERGE`. Follows the
//! pattern `network-dbt` uses for `enabled_carriers_inventory` over
//! `enabled_carriers_history` — latest-per-key picked with a `row_number()`
//! window, merged on the key — but it is our own job issuing the SQL, not a dbt
//! model.
//!
//! **Unlike the history table it holds only valid tickets.** History records
//! every ticket including refusals, so it answers "what happened"; this answers
//! "what is in force", and a refused ticket is not in force. Note that a refusal
//! does not revoke an earlier grant: a hotspot whose newest ticket was rejected
//! keeps the last valid one, which is what excluding refusals from the source
//! achieves.
//!
//! **Not written by the Rust iceberg writer.** It is maintained in place by
//! `MERGE`, which the append-only writer cannot express — hence the table being
//! unpartitioned, and hence the DDL existing only so the merge has a target.
//!
//! Readers: the burn path, the ticket CLI, and anyone asking what a hotspot's
//! multiplier is. The burn deliberately takes whatever is current here rather
//! than reconstructing what was in force when the data moved — see
//! `mobile_packet_verifier::multiplier::trino`.

use chrono::{DateTime, FixedOffset};
use helium_iceberg::{FieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::multiplier_ticket_history::{MultiplierDecimal, MULTIPLIER_PRECISION, MULTIPLIER_SCALE};
pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "multiplier_ticket_inventory";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergMultiplierInventory {
    pub hotspot_pubkey: String,
    pub multiplier: MultiplierDecimal,
    /// When the issuer signed the ticket that granted this multiplier — the
    /// value that decided it wins.
    pub signed_timestamp: DateTime<FixedOffset>,
    /// When ingest received that ticket.
    pub received_timestamp: DateTime<FixedOffset>,
    /// When the packet verifier accepted it.
    pub verified_timestamp: DateTime<FixedOffset>,
    pub signer: String,
    pub message: String,
}

/// Deliberately unpartitioned: rows are updated in place by `MERGE`, so there is
/// no append dimension to partition on.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("hotspot_pubkey"),
            FieldDefinition::required_decimal("multiplier", MULTIPLIER_PRECISION, MULTIPLIER_SCALE),
            FieldDefinition::required_timestamptz("signed_timestamp"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_timestamptz("verified_timestamp"),
            FieldDefinition::required_string("signer"),
            FieldDefinition::required_string("message"),
        ])
        .build()
}

/// The `MERGE` that brings the inventory up to date with the history.
///
/// `valid_status` is passed in rather than written here so it stays the proto
/// enum's own `as_str_name()`, and cannot drift from what the writer stored.
///
/// Rebuilds the source from the whole history each run rather than tracking a
/// watermark. Tickets are rare — HIP-150 expects a small number of granted
/// hotspots — so the scan is cheap and a full recompute cannot drift from the
/// history the way an incremental one can. Revisit if ticket volume ever makes
/// that untrue.
pub fn merge_statement(history_table: &str, inventory_table: &str, valid_status: &str) -> String {
    format!(
        r#"
        MERGE INTO {inventory_table} AS t
        USING (
            SELECT
                hotspot_pubkey,
                multiplier,
                signed_timestamp,
                received_timestamp,
                verified_timestamp,
                signer,
                message
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY hotspot_pubkey
                        ORDER BY signed_timestamp DESC, received_timestamp DESC
                    ) AS rn
                FROM {history_table}
                WHERE status = '{valid_status}'
                  AND multiplier IS NOT NULL
            )
            WHERE rn = 1
        ) AS s
        ON t.hotspot_pubkey = s.hotspot_pubkey
        WHEN MATCHED THEN UPDATE SET
            multiplier = s.multiplier,
            signed_timestamp = s.signed_timestamp,
            received_timestamp = s.received_timestamp,
            verified_timestamp = s.verified_timestamp,
            signer = s.signer,
            message = s.message
        WHEN NOT MATCHED THEN INSERT (
            hotspot_pubkey,
            multiplier,
            signed_timestamp,
            received_timestamp,
            verified_timestamp,
            signer,
            message
        ) VALUES (
            s.hotspot_pubkey,
            s.multiplier,
            s.signed_timestamp,
            s.received_timestamp,
            s.verified_timestamp,
            s.signer,
            s.message
        )
        "#
    )
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergMultiplierInventory>> {
    let all = trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await?
        .into_vec();
    Ok(all)
}
