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

use std::collections::HashMap;

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
///
/// `granted` is deduplicated on that key before the insert. `ON CONFLICT` only
/// resolves a collision with a row already in the table — Postgres refuses a
/// statement that proposes the same key twice in one command, with "ON CONFLICT
/// DO UPDATE command cannot affect row a second time", and that would abort the
/// whole file's transaction. One file can carry the same key twice easily
/// enough: a client that retransmits a signed ticket sends the same
/// `(hotspot, timestamp)` again, and both copies land in the same ingest roll.
///
/// Later wins, matching what `ON CONFLICT DO UPDATE` does across files, so a
/// duplicate resolves the same way whether or not it shares a file with the
/// grant it supersedes. [`crate::pending_burns::save_data_transfer_sessions`]
/// merges its own batch ahead of an upsert for the same reason.
pub async fn save(
    txn: &mut Transaction<'_, Postgres>,
    granted: &[GrantedMultiplier],
) -> anyhow::Result<()> {
    if granted.is_empty() {
        return Ok(());
    }

    // `HashMap` from an iterator of pairs keeps the last value for a repeated
    // key, which is the "later wins" above.
    let deduped: Vec<&GrantedMultiplier> = granted
        .iter()
        .map(|g| ((&g.hotspot_pubkey, g.effective_timestamp), g))
        .collect::<HashMap<_, _>>()
        .into_values()
        .collect();

    let hotspot_pubkeys: Vec<String> = deduped
        .iter()
        .map(|g| g.hotspot_pubkey.to_string())
        .collect();
    let multipliers: Vec<Decimal> = deduped.iter().map(|g| g.multiplier.as_decimal()).collect();
    let effective: Vec<DateTime<Utc>> = deduped.iter().map(|g| g.effective_timestamp).collect();

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
