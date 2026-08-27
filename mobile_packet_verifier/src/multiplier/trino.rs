//! Reading the multiplier currently in force per hotspot.
//!
//! **Nothing calls this yet.** It is the read side the burn path will use once
//! multipliers are applied to data credits; until then a ticket is recorded and
//! affects nothing.
//!
//! # Whatever is current when the session arrives
//!
//! A session is multiplied by whatever is in force when it is accumulated — not
//! by what was in force at the instant the data moved. Reconstructing the
//! latter would mean a point-in-time query against the ticket history, bounded
//! by the file's timestamp.
//!
//! That precision would be false. Sessions reach us batched behind an ingest
//! roll, tickets arrive on their own schedule, and the burn only runs hourly, so
//! the boundary between "before the ticket" and "after" is already fuzzy by
//! minutes. Paying for an exact answer to a question whose inputs are
//! approximate buys nothing, and it costs a scan of the whole history per file.
//!
//! So this reads the inventory table [`super::inventory`] keeps merged from the
//! history — one row per hotspot, already latest-per-key. No window function,
//! no timestamp bound.
//!
//! # What that trades away
//!
//! * **The inventory's refresh interval becomes visible in burns.** A ticket
//!   takes effect once the next merge has run, not the moment it is verified.
//!   Tune `inventory_refresh_interval` against how promptly a grant should land.
//! * **Replaying a backlog applies today's multipliers to old data.** Files
//!   reprocessed long after the fact are multiplied by what is in force now, not
//!   by what was in force then. Normal operation processes files promptly, so
//!   this shows up only after an outage or a deliberate replay.
//!
//! Both follow from the same decision, taken knowingly: the timing was never
//! exact, and pretending otherwise would cost more than it is worth.

use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
use helium_crypto::PublicKeyBinary;
use helium_iceberg_oracles::data_transfer::multiplier_ticket_inventory::{NAMESPACE, TABLE_NAME};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use super::Multipliers;

/// The multiplier in force per hotspot, as of the last inventory refresh.
///
/// Hotspots with no ticket are absent from the result; [`Multipliers::get`]
/// resolves those to [`DataTransferMultiplier::DEFAULT`].
pub async fn get_multipliers(trino: &trino_client::Client) -> anyhow::Result<Multipliers> {
    get_multipliers_from(trino, &format!("{NAMESPACE}.{TABLE_NAME}")).await
}

/// Like [`get_multipliers`], with an explicit table name.
///
/// Tests point this at a per-test catalog, the same way
/// [`crate::gateway::GatewayResolver::new_with_inventory_table`] does.
pub async fn get_multipliers_from(
    trino: &trino_client::Client,
    table: &str,
) -> anyhow::Result<Multipliers> {
    #[derive(Trino, Serialize, Deserialize)]
    struct Row {
        hotspot_pubkey: String,
        multiplier: String,
    }

    // The inventory is already one row per hotspot, so this is a plain read.
    //
    // `cast(... as varchar)` rather than reading a decimal: the value is
    // re-parsed into a validated `DataTransferMultiplier` below, and a decimal
    // string is the exact representation both sides already agree on.
    let stmt = trino_client::Statement::new(format!(
        "SELECT hotspot_pubkey, cast(multiplier AS varchar) AS multiplier FROM {table}"
    ))
    .typed::<Row>();

    let rows = trino.get_all(stmt).await?;

    let mut multipliers = Multipliers::default();
    for row in rows {
        let hotspot_pubkey: PublicKeyBinary = match row.hotspot_pubkey.parse() {
            Ok(pubkey) => pubkey,
            Err(err) => {
                tracing::warn!(pub_key = %row.hotspot_pubkey, ?err, "skipping unparseable hotspot");
                continue;
            }
        };

        // Values were validated before being written, so a value that no longer
        // parses means the accepted range was narrowed since. Skip it rather
        // than fail the file: the hotspot falls back to the default, which is
        // the conservative direction.
        match row
            .multiplier
            .parse()
            .map_err(anyhow::Error::from)
            .and_then(|value| -> anyhow::Result<_> { Ok(DataTransferMultiplier::new(value)?) })
        {
            Ok(multiplier) => multipliers.insert(hotspot_pubkey, multiplier),
            Err(err) => tracing::warn!(
                %hotspot_pubkey, multiplier = %row.multiplier, ?err,
                "stored multiplier is no longer valid, treating hotspot as unmultiplied"
            ),
        }
    }

    Ok(multipliers)
}
