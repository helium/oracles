use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store_oracles::{
    mobile_session::DataTransferSessionReq, mobile_transfer::ValidDataTransferSession,
};
use helium_crypto::PublicKeyBinary;
use sqlx::{prelude::FromRow, Pool, Postgres, Row, Transaction};

use crate::bytes_to_dc;

const METRIC_NAME: &str = "pending_dc_burn";

#[derive(Debug, Clone, FromRow, PartialEq)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

impl DataTransferSession {
    pub fn dc_to_burn(&self) -> u64 {
        bytes_to_dc(self.rewardable_bytes as u64)
    }

    pub fn from_req(req: &DataTransferSessionReq, last_timestamp: DateTime<Utc>) -> Self {
        DataTransferSession {
            pub_key: req.data_transfer_usage.pub_key.clone(),
            payer: req.data_transfer_usage.payer.clone(),
            uploaded_bytes: req.data_transfer_usage.upload_bytes as i64,
            downloaded_bytes: req.data_transfer_usage.download_bytes as i64,
            rewardable_bytes: req.rewardable_bytes as i64,
            // timestamps are the same upon ingest
            first_timestamp: last_timestamp,
            last_timestamp,
        }
    }
}

impl From<DataTransferSession> for ValidDataTransferSession {
    fn from(value: DataTransferSession) -> Self {
        let num_dcs = value.dc_to_burn();

        ValidDataTransferSession {
            pub_key: value.pub_key,
            payer: value.payer,
            upload_bytes: value.uploaded_bytes as u64,
            download_bytes: value.downloaded_bytes as u64,
            rewardable_bytes: value.rewardable_bytes as u64,
            num_dcs,
            first_timestamp: value.first_timestamp,
            last_timestamp: value.last_timestamp,
            burn_timestamp: Utc::now(),
        }
    }
}

pub struct PendingPayerBurn {
    pub payer: PublicKeyBinary,
    pub total_dcs: u64,
    pub sessions: Vec<DataTransferSession>,
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

pub async fn get_all(conn: &Pool<Postgres>) -> anyhow::Result<Vec<DataTransferSession>> {
    let results = sqlx::query_as("SELECT * FROM data_transfer_sessions")
        .fetch_all(conn)
        .await?;

    Ok(results)
}

pub async fn get_all_payer_burns(conn: &Pool<Postgres>) -> anyhow::Result<Vec<PendingPayerBurn>> {
    let pending_payer_burns = get_all(conn)
        .await?
        .into_iter()
        .fold(
            HashMap::<PublicKeyBinary, PendingPayerBurn>::new(),
            |mut map, session| {
                let dc_to_burn = session.dc_to_burn();

                match map.get_mut(&session.payer) {
                    Some(pending_payer_burn) => {
                        pending_payer_burn.total_dcs += dc_to_burn;
                        pending_payer_burn.sessions.push(session);
                    }
                    None => {
                        map.insert(
                            session.payer.clone(),
                            PendingPayerBurn {
                                payer: session.payer.clone(),
                                total_dcs: dc_to_burn,
                                sessions: vec![session],
                            },
                        );
                    }
                }

                map
            },
        )
        .into_values()
        .collect();

    Ok(pending_payer_burns)
}

pub async fn save_data_transfer_sessions(
    txn: &mut Transaction<'_, Postgres>,
    data_transfer_session: &[DataTransferSession],
) -> anyhow::Result<()> {
    let mut merged = HashMap::new();
    for session in data_transfer_session {
        merged
            .entry((&session.pub_key, &session.payer))
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
        ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            first_timestamp = LEAST(data_transfer_sessions.first_timestamp, EXCLUDED.first_timestamp),
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
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

fn set_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).set(value as f64);
}

pub fn increment_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).increment(value as f64);
}

pub fn decrement_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).decrement(value as f64);
}
