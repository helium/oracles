//! Test fixture for the `hotspots.bans` Iceberg table that mobile-verifier
//! writes and `BanResolver` reads.
//!
//! The [`IcebergTestHarness`] registers a uniquely-named catalog per test, so
//! the production `mobile` catalog prefix isn't available. The table resolves
//! as [`RESOLVER_TABLE`] against the Trino client's default catalog.

use file_store_oracles::mobile_ban::VerifiedBanReport;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::hotspots::{ban, IcebergBan, NAMESPACE};
use mobile_packet_verifier::banning::BanResolver;

/// Two-part `schema.table` name the resolver should query in tests.
pub const RESOLVER_TABLE: &str = "hotspots.bans";

/// Record ban reports the way mobile-verifier does: valid reports go to
/// `hotspots.bans`, anything else is left out.
pub async fn record(
    harness: &IcebergTestHarness,
    reports: impl IntoIterator<Item = VerifiedBanReport>,
) -> anyhow::Result<()> {
    let rows = reports
        .into_iter()
        .filter(|report| report.is_valid())
        .map(|report| IcebergBan::from(&report))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(());
    }
    let writer = harness
        .get_table_writer_in::<IcebergBan>(NAMESPACE, ban::TABLE_NAME)
        .await?;
    writer.write(rows).await?;
    Ok(())
}

/// A real [`BanResolver`] backed by the harness's Trino, pointed at the
/// per-test `hotspots.bans` table.
pub async fn resolver(harness: &IcebergTestHarness) -> anyhow::Result<BanResolver> {
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    Ok(BanResolver::new_with_bans_table(trino, RESOLVER_TABLE))
}
