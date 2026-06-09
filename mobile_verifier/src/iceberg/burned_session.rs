//! Trino reader for the `data_transfer.burned_sessions` table.
//!
//! `mobile_packet_verifier` writes this table from the same
//! `ValidDataTransferSession` data that populates the Postgres
//! `hotspot_data_transfer_sessions` table. This module is the Trino side of the
//! strangler migration in [`crate::data_session`]: it returns the same
//! [`HotspotMap`] the reward pipeline already consumes, with the per-hotspot
//! aggregation pushed down into Trino.

use std::ops::Range;

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use crate::data_session::HotspotMap;

pub const NAMESPACE: &str = "data_transfer";
pub const TABLE_NAME: &str = "burned_sessions";

/// One row per hotspot — the `num_dcs`/`rewardable_bytes` sums for the epoch.
#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
struct BurnedSessionAggRow {
    pub_key: String,
    rewardable_dc: u64,
    rewardable_bytes: u64,
}

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
struct CountRow {
    n: i64,
}

/// Statement that sums DC and rewardable bytes per hotspot over the half-open
/// `[start, end)` epoch (matching the Postgres query in [`crate::data_session`]).
fn agg_statement(epoch: &Range<DateTime<Utc>>) -> trino_client::Statement {
    trino_client::Statement::new(format!(
        "SELECT pub_key, \
                SUM(num_dcs) AS rewardable_dc, \
                SUM(rewardable_bytes) AS rewardable_bytes \
         FROM {NAMESPACE}.{TABLE_NAME} \
         WHERE burn_timestamp >= :start AND burn_timestamp < :end \
         GROUP BY pub_key"
    ))
    .bind("start", epoch.start)
    .bind("end", epoch.end)
}

/// Aggregate the burned data-transfer sessions for `epoch` into a [`HotspotMap`].
///
/// Mirrors [`crate::data_session::aggregate_hotspot_data_sessions_to_dc`] but
/// reads from Trino. No `COALESCE` is needed: `rewardable_bytes` is non-nullable
/// in the iceberg schema (unlike Postgres). An empty epoch yields an empty map.
pub async fn aggregate_hotspot_data_sessions_to_dc(
    trino: &trino_client::Client,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<HotspotMap> {
    let rows: Vec<BurnedSessionAggRow> = trino
        .get_all(agg_statement(epoch).typed::<BurnedSessionAggRow>())
        .await?;

    let mut map = HotspotMap::new();
    for row in rows {
        let pub_key: PublicKeyBinary = row.pub_key.parse()?;
        let reward = map.entry(pub_key).or_default();
        reward.rewardable_dc += row.rewardable_dc;
        reward.rewardable_bytes += row.rewardable_bytes;
    }
    Ok(map)
}

/// True when no burned sessions exist past the end of the reward period — i.e.
/// the burn/write pipeline isn't current through the period we want to reward.
///
/// Trino analogue of [`crate::rewarder::db::no_speedtests`] /
/// [`crate::rewarder::db::no_wifi_heartbeats`].
pub async fn no_burned_sessions(
    trino: &trino_client::Client,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let stmt = trino_client::Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {NAMESPACE}.{TABLE_NAME} WHERE burn_timestamp >= :end"
    ))
    .bind("end", reward_period.end)
    .typed::<CountRow>();

    let count = trino.get_all(stmt).await?.first().map_or(0, |row| row.n);
    Ok(count == 0)
}

/// Schema of `data_transfer.burned_sessions`, kept in sync with the writer in
/// `mobile_packet_verifier::iceberg::burned_session`. Owned here so this crate
/// can read (and tests can create) the table without depending on the packet
/// verifier.
pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pub_key"),
            FieldDefinition::required_string("payer"),
            FieldDefinition::required_long("upload_bytes"),
            FieldDefinition::required_long("download_bytes"),
            FieldDefinition::required_long("rewardable_bytes"),
            FieldDefinition::required_long("num_dcs"),
            FieldDefinition::required_timestamptz("first_timestamp"),
            FieldDefinition::required_timestamptz("last_timestamp"),
            FieldDefinition::required_timestamptz("burn_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "burn_timestamp",
            "burn_timestamp_day",
        ))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn agg_statement_filters_half_open_epoch_and_groups_by_hotspot() {
        let epoch = utc(2024, 1, 15)..utc(2024, 1, 16);
        let rendered = agg_statement(&epoch).render().unwrap();

        // Aggregation pushed into Trino.
        assert!(rendered.contains("GROUP BY pub_key"), "{rendered}");
        // Half-open range on the partition column.
        assert!(rendered.contains("burn_timestamp >= ?"), "{rendered}");
        assert!(rendered.contains("burn_timestamp < ?"), "{rendered}");
        // Partition-friendly, UTC-tagged timestamp literals.
        assert!(
            rendered.contains("TIMESTAMP '2024-01-15 00:00:00 UTC'"),
            "{rendered}"
        );
        assert!(
            rendered.contains("TIMESTAMP '2024-01-16 00:00:00 UTC'"),
            "{rendered}"
        );
    }

    #[test]
    fn table_definition_builds() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), NAMESPACE);
    }
}
