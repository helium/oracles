//! Trino-backed hotspot bans.
//!
//! mobile-verifier writes every valid ban and unban report to the
//! `hotspots.bans` Iceberg table. [`BanResolver`] reads that table to find the
//! hotspots banned as of an ingest file's timestamp. Their data transfer is
//! still recorded as valid, but with zero rewardable bytes, so nothing is
//! burned or rewarded for it.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;

/// Fully-qualified `catalog.schema.table` for the bans written by
/// mobile-verifier.
pub const BANS_TABLE: &str = "mobile.hotspots.bans";

#[derive(Debug, Default)]
pub struct BannedRadios {
    banned: HashSet<PublicKeyBinary>,
}

impl BannedRadios {
    pub fn contains(&self, hotspot_pubkey: &PublicKeyBinary) -> bool {
        self.banned.contains(hotspot_pubkey)
    }
}

#[derive(Clone)]
pub struct BanResolver {
    trino_client: trino_client::Client,
    bans_table: String,
}

impl BanResolver {
    pub fn new(trino_client: trino_client::Client) -> Self {
        Self::new_with_bans_table(trino_client, BANS_TABLE)
    }

    /// Like [`new`](Self::new), but with an explicit bans table name.
    ///
    /// Tests use this to point at a per-test iceberg catalog, passing the
    /// two-part `hotspots.bans` name, which resolves against the Trino client's
    /// default catalog.
    pub fn new_with_bans_table(
        trino_client: trino_client::Client,
        bans_table: impl Into<String>,
    ) -> Self {
        Self {
            trino_client,
            bans_table: bans_table.into(),
        }
    }

    /// The hotspots whose data transfer is banned as of `timestamp`.
    ///
    /// A ban counts when it was received _before_ `timestamp` (`<`) and has not
    /// expired by it (`>=`). An unban lifts every ban received before it. Of the
    /// bans left, the latest decides, and only `all` and `data` bans affect data
    /// transfer. If a hotspot was banned for POC yesterday and for DATA today,
    /// files from yesterday do not ban it.
    ///
    /// This matches how the bans were applied when they were read from s3 into
    /// Postgres.
    pub async fn banned_radios(&self, timestamp: DateTime<Utc>) -> anyhow::Result<BannedRadios> {
        use trino_rust_client::Trino;
        #[derive(Trino, serde::Serialize, serde::Deserialize)]
        struct Row {
            pubkey: String,
        }

        let stmt = trino_client::Statement::new(format!(
            r#"
            WITH events AS (
                SELECT pubkey, received_timestamp, is_ban, ban_type, expiration_timestamp
                FROM {}
                WHERE received_timestamp < :timestamp
            ),
            last_unban AS (
                SELECT pubkey, max(received_timestamp) AS unbanned_at
                FROM events
                WHERE NOT is_ban
                GROUP BY pubkey
            ),
            active AS (
                SELECT
                    e.pubkey,
                    e.ban_type,
                    row_number() OVER (
                        PARTITION BY e.pubkey ORDER BY e.received_timestamp DESC
                    ) AS rn
                FROM events e
                LEFT JOIN last_unban u ON u.pubkey = e.pubkey
                WHERE e.is_ban
                    AND (e.expiration_timestamp IS NULL
                        OR e.expiration_timestamp >= :timestamp)
                    AND (u.unbanned_at IS NULL OR e.received_timestamp > u.unbanned_at)
            )
            SELECT pubkey
            FROM active
            WHERE rn = 1 AND ban_type IN ('all', 'data')
            "#,
            self.bans_table
        ))
        .bind("timestamp", timestamp)
        .typed::<Row>();

        let rows = self.trino_client.get_all(stmt).await?;

        let mut banned = HashSet::with_capacity(rows.len());
        for row in rows {
            match row.pubkey.parse::<PublicKeyBinary>() {
                Ok(pubkey) => {
                    banned.insert(pubkey);
                }
                Err(err) => {
                    tracing::warn!(?err, pubkey = %row.pubkey, "skipping unparseable banned pubkey")
                }
            }
        }
        Ok(BannedRadios { banned })
    }
}
