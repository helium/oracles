use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store::{mobile_session::DataTransferSessionReq, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use sqlx::{prelude::FromRow, Pool, Postgres, Row, Transaction};

use crate::bytes_to_dc;

const METRIC_NAME: &str = "pending_dc_burn";

#[derive(FromRow)]
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
}

impl From<DataTransferSession> for ValidDataTransferSession {
    fn from(session: DataTransferSession) -> Self {
        let num_dcs = session.dc_to_burn();

        ValidDataTransferSession {
            pub_key: session.pub_key.into(),
            payer: session.payer.into(),
            upload_bytes: session.uploaded_bytes as u64,
            download_bytes: session.downloaded_bytes as u64,
            rewardable_bytes: session.rewardable_bytes as u64,
            num_dcs,
            first_timestamp: session.first_timestamp.encode_timestamp_millis(),
            last_timestamp: session.last_timestamp.encode_timestamp_millis(),
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
    sqlx::query_as("SELECT * FROM data_transfer_sessions")
        .fetch_all(conn)
        .await
        .map_err(anyhow::Error::from)
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

pub async fn save_data_transfer_session_req(
    txn: &mut Transaction<'_, Postgres>,
    req: &DataTransferSessionReq,
    last_timestamp: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    save_data_transfer_session(
        txn,
        &DataTransferSession {
            pub_key: req.data_transfer_usage.pub_key.clone(),
            payer: req.data_transfer_usage.payer.clone(),
            uploaded_bytes: req.data_transfer_usage.upload_bytes as i64,
            downloaded_bytes: req.data_transfer_usage.download_bytes as i64,
            rewardable_bytes: req.rewardable_bytes as i64,
            // timestamps are the same upon ingest
            first_timestamp: last_timestamp,
            last_timestamp,
        },
    )
    .await?;

    Ok(())
}

pub async fn save_data_transfer_session(
    txn: &mut Transaction<'_, Postgres>,
    data_transfer_session: &DataTransferSession,
) -> Result<(), sqlx::Error> {
    sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions
                (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
                uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
                downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
                rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
                first_timestamp = LEAST(data_transfer_sessions.first_timestamp, EXCLUDED.first_timestamp),
                last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
            "#
        )
            .bind(&data_transfer_session.pub_key)
            .bind(&data_transfer_session.payer)
            .bind(data_transfer_session.uploaded_bytes)
            .bind(data_transfer_session.downloaded_bytes)
            .bind(data_transfer_session.rewardable_bytes)
            .bind(data_transfer_session.first_timestamp)
            .bind(data_transfer_session.last_timestamp)
            .execute(txn)
            .await?;

    Ok(())
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
