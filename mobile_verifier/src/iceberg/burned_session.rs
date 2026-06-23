//! Trino reader for the `data_transfer.burned_sessions` table.
//!
//! The table's schema (struct + `table_definition`) is owned by the shared
//! [`helium_iceberg_oracles::data_transfer::burned_session`] crate and written
//! by `mobile_packet_verifier`. This module holds only the read side: the
//! per-hotspot aggregation the reward pipeline needs, returning the same
//! [`RewardableDataByHotspot`] as the Postgres path in [`crate::data_session`].

use std::ops::Range;

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg_oracles::data_transfer::burned_session::{NAMESPACE, TABLE_NAME};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

use crate::data_session::RewardableDataByHotspot;

/// Aggregate the burned data-transfer sessions for `epoch` into a
/// [`RewardableDataByHotspot`].
///
/// Mirrors [`crate::data_session::aggregate_hotspot_data_sessions_to_dc`] but
/// reads from Trino. No `COALESCE` is needed: `rewardable_bytes` is non-nullable
/// in the iceberg schema (unlike Postgres). An empty epoch yields an empty map.
pub async fn aggregate_hotspot_data_sessions_to_dc(
    trino: &trino_client::Client,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<RewardableDataByHotspot> {
    // Column names must match the `SELECT ... AS` aliases in `aggregate_statement`.
    #[derive(Trino, Serialize, Deserialize)]
    struct Row {
        pub_key: String,
        rewardable_dc: u64,
        rewardable_bytes: u64,
    }

    let rows = trino
        .get_all(aggregate_statement(epoch).typed::<Row>())
        .await?;

    let mut map = RewardableDataByHotspot::default();
    for row in rows {
        let pub_key: PublicKeyBinary = row.pub_key.parse()?;
        let totals = map.entry(pub_key).or_default();
        totals.rewardable_dc += row.rewardable_dc;
        totals.rewardable_bytes += row.rewardable_bytes;
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
    #[derive(Trino, Serialize, Deserialize)]
    struct Count {
        n: i64,
    }

    let stmt = trino_client::Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {NAMESPACE}.{TABLE_NAME} WHERE burn_timestamp >= :end"
    ))
    .bind("end", reward_period.end)
    .typed::<Count>();

    let count = trino.get_all(stmt).await?.first().map_or(0, |row| row.n);
    Ok(count == 0)
}

/// Statement that sums DC and rewardable bytes per hotspot over the half-open
/// `[start, end)` epoch (matching the Postgres query in [`crate::data_session`]).
fn aggregate_statement(epoch: &Range<DateTime<Utc>>) -> trino_client::Statement {
    trino_client::Statement::new(format!(
        "SELECT pub_key,
                SUM(num_dcs) AS rewardable_dc,
                SUM(rewardable_bytes) AS rewardable_bytes
         FROM {NAMESPACE}.{TABLE_NAME}
         WHERE burn_timestamp >= :start AND burn_timestamp < :end
         GROUP BY pub_key"
    ))
    .bind("start", epoch.start)
    .bind("end", epoch.end)
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
    fn aggregate_statement_filters_half_open_epoch_and_groups_by_hotspot() {
        let epoch = utc(2024, 1, 15)..utc(2024, 1, 16);
        let rendered = aggregate_statement(&epoch).render().unwrap();

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
}
