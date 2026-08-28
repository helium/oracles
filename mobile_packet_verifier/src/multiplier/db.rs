//! Which multipliers have been granted, and from when.
//!
//! Verified tickets go two places. The Iceberg history in [`super::ingestor`] is
//! the audit record: every ticket, refusals included, with its verdict. This
//! table is what the burn uses. It holds grants only, and it is in Postgres so
//! that pricing a session is a join against `data_transfer_sessions` rather than
//! a lookup in another store.
//!
//! Append-only, because the burn asks what was in force when the data moved, not
//! what is in force now. See [`crate::pending_burns::get_all`].

use chrono::{DateTime, Utc};
use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
use helium_crypto::PublicKeyBinary;
use rust_decimal::Decimal;
use sqlx::{Postgres, Transaction};

/// One grant: which hotspot, how much, and from when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedMultiplier {
    pub hotspot_pubkey: PublicKeyBinary,
    pub multiplier: DataTransferMultiplier,
    /// The timestamp the issuer signed. See migration 10 for why this rather
    /// than the time ingest received the ticket.
    pub effective_timestamp: DateTime<Utc>,
}

/// Record granted multipliers.
///
/// Runs in the caller's transaction, next to the file poller's record that the
/// file was processed, so a file is never marked done without its grants.
///
/// Keyed on `(hotspot_pubkey, effective_timestamp)`, so reprocessing a file
/// rewrites the same rows instead of adding to them.
pub async fn save(
    txn: &mut Transaction<'_, Postgres>,
    granted: &[GrantedMultiplier],
) -> anyhow::Result<()> {
    if granted.is_empty() {
        return Ok(());
    }

    let hotspot_pubkeys: Vec<String> = granted
        .iter()
        .map(|g| g.hotspot_pubkey.to_string())
        .collect();
    let multipliers: Vec<Decimal> = granted.iter().map(|g| g.multiplier.as_decimal()).collect();
    let effective: Vec<DateTime<Utc>> = granted.iter().map(|g| g.effective_timestamp).collect();

    sqlx::query(
        r#"
        INSERT INTO data_transfer_multipliers (hotspot_pubkey, multiplier, effective_timestamp)
        SELECT hotspot_pubkey, multiplier, effective_timestamp
        FROM UNNEST($1::text[], $2::numeric[], $3::timestamptz[])
            AS t(hotspot_pubkey, multiplier, effective_timestamp)
        ON CONFLICT (hotspot_pubkey, effective_timestamp) DO UPDATE SET
            multiplier = EXCLUDED.multiplier
        "#,
    )
    .bind(hotspot_pubkeys)
    .bind(multipliers)
    .bind(effective)
    .execute(&mut **txn)
    .await?;

    Ok(())
}
