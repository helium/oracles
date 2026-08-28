use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store_oracles::{
    mobile_session::DataTransferSessionReq, mobile_transfer::ValidDataTransferSession,
};
use helium_crypto::PublicKeyBinary;
use sqlx::{prelude::FromRow, Pool, Postgres, Row, Transaction};

use anyhow::Context;
use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
use rust_decimal::Decimal;

use crate::bytes_to_dc;

const METRIC_NAME: &str = "pending_dc_burn";

#[derive(Debug, Clone, FromRow, PartialEq)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    /// HIP-150: the multiplier this row's bytes are priced at.
    ///
    /// There is no such column on `data_transfer_sessions`. [`get_all`] works it
    /// out by joining the ticket history at the row's own timestamp.
    ///
    /// There is one on `pending_data_transfer_sessions`, because a row that has
    /// reached that table has a price already fixed on chain.
    ///
    /// A plain `Decimal` because that is what Postgres returns.
    /// [`DataTransferSession::dc_to_burn`] validates it before use.
    multiplier: Decimal,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

impl DataTransferSession {
    /// Data credits to burn: bytes converted once, then multiplied.
    ///
    /// Call this on a group, not a raw row. `bytes_to_dc` rounds up off a
    /// one-DC floor, so the number of times it runs affects what a payer pays.
    /// [`group_by_multiplier`] decides that; this is just the arithmetic.
    ///
    /// It fails if the multiplier is outside the accepted range, or if the
    /// multiplication overflows. Neither should happen: the first needs the
    /// range to have been narrowed since the ticket was recorded, the second a
    /// DC count near `u64::MAX`. Both abort the burn from
    /// [`get_all_payer_burns`], before any transaction exists, so nothing is
    /// spent and the rows wait for the next attempt.
    pub fn dc_to_burn(&self) -> anyhow::Result<u64> {
        let multiplier = DataTransferMultiplier::new(self.multiplier).with_context(|| {
            format!(
                "multiplier {} for {} is not acceptable",
                self.multiplier, self.pub_key
            )
        })?;

        Ok(multiplier.apply(bytes_to_dc(self.rewardable_bytes as u64))?)
    }

    pub fn from_req(req: &DataTransferSessionReq, last_timestamp: DateTime<Utc>) -> Self {
        DataTransferSession {
            pub_key: req.data_transfer_usage.pub_key.clone(),
            payer: req.data_transfer_usage.payer.clone(),
            uploaded_bytes: req.data_transfer_usage.upload_bytes as i64,
            downloaded_bytes: req.data_transfer_usage.download_bytes as i64,
            rewardable_bytes: req.rewardable_bytes as i64,
            // Unpriced. Nothing writes this to `data_transfer_sessions` --
            // there is no column for it -- and the burn resolves it on read.
            multiplier: DataTransferMultiplier::DEFAULT.as_decimal(),
            // timestamps are the same upon ingest
            first_timestamp: last_timestamp,
            last_timestamp,
        }
    }

    pub fn pub_key(&self) -> &PublicKeyBinary {
        &self.pub_key
    }

    pub fn multiplier(&self) -> Decimal {
        self.multiplier
    }

    pub fn last_timestamp(&self) -> DateTime<Utc> {
        self.last_timestamp
    }
}

/// Fallible because [`DataTransferSession::dc_to_burn`] is. The burned record
/// states what was charged, so it cannot be written without a price.
impl TryFrom<DataTransferSession> for ValidDataTransferSession {
    type Error = anyhow::Error;

    fn try_from(session: DataTransferSession) -> anyhow::Result<Self> {
        let num_dcs = session.dc_to_burn()?;
        let multiplier = DataTransferMultiplier::new(session.multiplier)?;

        Ok(ValidDataTransferSession {
            pub_key: session.pub_key,
            payer: session.payer,
            upload_bytes: session.uploaded_bytes as u64,
            download_bytes: session.downloaded_bytes as u64,
            rewardable_bytes: session.rewardable_bytes as u64,
            num_dcs,
            multiplier,
            first_timestamp: session.first_timestamp,
            last_timestamp: session.last_timestamp,
            burn_timestamp: Utc::now(),
        })
    }
}

pub struct PendingPayerBurn {
    pub payer: PublicKeyBinary,
    /// What the payer is charged: the sum over [`Self::sessions`].
    pub total_dcs: u64,
    /// The per-file rows, priced.
    ///
    /// The burn moves these into `pending_data_transfer_sessions` rather than
    /// the groups. A group spans several files, so putting one back as a single
    /// row would lose those boundaries, and a retry would then price all of it
    /// at one rate.
    pub rows: Vec<DataTransferSession>,
}

impl PendingPayerBurn {
    /// One session per `(hotspot, multiplier)`. Each becomes one burned record,
    /// so a hotspot billed at two rates produces two records.
    ///
    /// Derived rather than stored, so it cannot drift from [`Self::rows`] or
    /// from the `total_dcs` computed off it.
    pub fn sessions(&self) -> Vec<DataTransferSession> {
        group_by_multiplier(&self.rows)
    }
}

pub async fn initialize(conn: &Pool<Postgres>) -> anyhow::Result<()> {
    let results = sqlx::query(
        r#"
        SELECT payer, sum(rewardable_bytes)::bigint as total_rewardable_bytes
        FROM data_transfer_sessions
        GROUP BY payer
        "#,
    )
    .fetch_all(conn)
    .await?;

    for row in results {
        let payer: PublicKeyBinary = row.get("payer");
        let total_rewardable_bytes: u64 = row.get::<i64, _>("total_rewardable_bytes") as u64;

        set_metric(&payer, bytes_to_dc(total_rewardable_bytes));
    }

    Ok(())
}

/// Every accumulated row, with the multiplier that applies to it.
///
/// The multiplier is looked up here rather than stored on the row. A row is
/// written when its file is processed; the ticket that should apply to it may
/// not have been processed yet. By burn time we know more.
///
/// Each row covers one hotspot in one file, and all reports in a file share the
/// file's timestamp. So `last_timestamp` is a single point in time, not a range.
///
/// The join takes the newest grant effective at or before that point. Later
/// grants don't apply. A hotspot with no grant matches nothing and gets 1.
///
/// These are the raw rows. [`get_all_payer_burns`] groups them before converting
/// anything to data credits.
pub async fn get_all(conn: &Pool<Postgres>) -> anyhow::Result<Vec<DataTransferSession>> {
    sqlx::query_as(
        r#"
        SELECT
            dts.pub_key,
            dts.payer,
            dts.uploaded_bytes,
            dts.downloaded_bytes,
            dts.rewardable_bytes,
            COALESCE(m.multiplier, 1) AS multiplier,
            dts.first_timestamp,
            dts.last_timestamp
        FROM data_transfer_sessions dts
        LEFT JOIN LATERAL (
            SELECT dtm.multiplier
            FROM data_transfer_multipliers dtm
            WHERE dtm.hotspot_pubkey = dts.pub_key
              AND dtm.effective_timestamp <= dts.last_timestamp
            ORDER BY dtm.effective_timestamp DESC
            LIMIT 1
        ) m ON TRUE
        "#,
    )
    .fetch_all(conn)
    .await
    .map_err(anyhow::Error::from)
}

/// Collapse priced rows into one session per `(hotspot, multiplier)`.
///
/// This has to happen before the data credit conversion. `bytes_to_dc` carries
/// two penalties -- a full DC for anything under 100KB, and a round up to the
/// next whole DC above that -- and you pay them once per conversion. Ten
/// 10,000-byte sessions summed are 100,000 bytes, so 1 DC. Converted one at a
/// time they are 10, because each one hits the floor by itself.
///
/// It matters to the deployer too. `floor(1 DC x 1.5)` is 1, so multiplying
/// per session would erase any multiplier below 2x on a single-DC session.
/// Grouping lets the fractions add up to whole data credits before the floor
/// lands on them.
///
/// Grouping first means bytes billed at the same rate are summed and converted
/// once, the way they were before HIP-150. Bytes billed at different rates end
/// up in different groups and convert separately, which is unavoidable: one
/// conversion cannot produce two rates.
///
/// So an unticketed hotspot rounds exactly as it always did, and a hotspot whose
/// multiplier changed mid-window pays one extra rounding.
pub(crate) fn group_by_multiplier(rows: &[DataTransferSession]) -> Vec<DataTransferSession> {
    let mut grouped: HashMap<_, DataTransferSession> = HashMap::new();

    for row in rows {
        grouped
            .entry((row.pub_key.clone(), row.payer.clone(), row.multiplier))
            .and_modify(|existing| merge_session(existing, row))
            .or_insert_with(|| row.clone());
    }

    grouped.into_values().collect()
}

/// What each payer owes, and the rows behind it.
///
/// Rows are priced one at a time, each at its own instant, then grouped by the
/// multiplier that came back, then converted to data credits once per group.
pub async fn get_all_payer_burns(conn: &Pool<Postgres>) -> anyhow::Result<Vec<PendingPayerBurn>> {
    let mut by_payer = HashMap::<PublicKeyBinary, Vec<DataTransferSession>>::new();
    for row in get_all(conn).await? {
        by_payer.entry(row.payer.clone()).or_default().push(row);
    }

    let mut burns = Vec::with_capacity(by_payer.len());
    for (payer, rows) in by_payer {
        // Fail the whole burn rather than skip a group. A session we cannot
        // price is not one to leave quietly out of a payer's bill. Nothing has
        // been spent at this point.
        let mut total_dcs = 0u64;
        for session in group_by_multiplier(&rows) {
            total_dcs += session.dc_to_burn()?;
        }

        // Set the gauge to what this payer actually owes. This is the first
        // point where that is known: `accumulate` increments as bytes arrive,
        // but it has no multiplier to apply and it groups by payer rather than
        // by hotspot, so its running total is only an estimate.
        //
        // This runs before the balance check in `Burner::burn`, so a payer that
        // cannot pay keeps a gauge showing its real debt, growing every cycle it
        // fails to burn. That is the point of the metric.
        set_metric(&payer, total_dcs);

        burns.push(PendingPayerBurn {
            payer,
            total_dcs,
            rows,
        });
    }

    Ok(burns)
}

pub async fn save_data_transfer_sessions(
    txn: &mut Transaction<'_, Postgres>,
    data_transfer_session: &[DataTransferSession],
) -> anyhow::Result<()> {
    // Keyed on the timestamp as well as the pair, matching the table. Reports
    // from one file share a timestamp and merge; reports from different files
    // stay in separate rows, so each row belongs to one instant.
    let mut merged = HashMap::new();
    for session in data_transfer_session {
        merged
            .entry((&session.pub_key, &session.payer, session.last_timestamp))
            .and_modify(|existing| merge_session(existing, session))
            .or_insert_with(|| session.clone());
    }
    let sessions = merged.into_values().collect::<Vec<_>>();

    let pub_keys = collect_field(&sessions, |s| s.pub_key.to_string());
    let payers = collect_field(&sessions, |s| s.payer.to_string());
    let uploaded = collect_field(&sessions, |s| s.uploaded_bytes);
    let downloaded = collect_field(&sessions, |s| s.downloaded_bytes);
    let rewardable = collect_field(&sessions, |s| s.rewardable_bytes);
    let first_ts = collect_field(&sessions, |s| s.first_timestamp);
    let last_ts = collect_field(&sessions, |s| s.last_timestamp);

    // Any multiplier on these rows is dropped. They have either never been
    // priced, or are coming back from a failed burn. Either way the next burn
    // prices them again, against a ticket history that may have grown since.
    sqlx::query(
            r#"
        INSERT INTO data_transfer_sessions
            (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
        SELECT
            pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp
        FROM UNNEST(
            $1::text[],
            $2::text[],
            $3::bigint[],
            $4::bigint[],
            $5::bigint[],
            $6::timestamptz[],
            $7::timestamptz[]
        ) AS t(
            pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp
        )
        ON CONFLICT (pub_key, payer, last_timestamp) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            first_timestamp = LEAST(data_transfer_sessions.first_timestamp, EXCLUDED.first_timestamp)
        "#
        )
        .bind(pub_keys)
        .bind(payers)
        .bind(uploaded)
        .bind(downloaded)
        .bind(rewardable)
        .bind(first_ts)
        .bind(last_ts)
        .execute(&mut **txn)
        .await?;

    Ok(())
}

fn collect_field<In, Out>(coll: &[In], field_fn: impl FnMut(&In) -> Out) -> Vec<Out> {
    coll.iter().map(field_fn).collect()
}

fn merge_session(existing: &mut DataTransferSession, other: &DataTransferSession) {
    existing.uploaded_bytes += other.uploaded_bytes;
    existing.downloaded_bytes += other.downloaded_bytes;
    existing.rewardable_bytes += other.rewardable_bytes;
    existing.first_timestamp = existing.first_timestamp.min(other.first_timestamp);
    existing.last_timestamp = existing.last_timestamp.max(other.last_timestamp);
}

pub async fn delete_for_payer(
    conn: &Pool<Postgres>,
    payer: &PublicKeyBinary,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM data_transfer_sessions WHERE payer = $1")
        .bind(payer)
        .execute(conn)
        .await?;

    Ok(())
}

pub fn set_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).set(value as f64);
}

pub fn increment_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).increment(value as f64);
}
