use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store::{mobile_session::DataTransferSessionReq, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::Signature;
use sqlx::{postgres::PgRow, prelude::FromRow, PgPool, Pool, Postgres, Row, Transaction};

const METRIC_NAME: &str = "pending_dc_burn";

#[derive(Debug, FromRow, Clone)]
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

#[derive(Debug)]
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

pub async fn get_pending_data_sessions_for_signature(
    conn: &PgPool,
    signature: &Signature,
) -> anyhow::Result<Vec<DataTransferSession>> {
    let pending = sqlx::query_as(
        r#"
        SELECT * FROM pending_data_transfer_sessions 
        WHERE signature = $1
        "#,
    )
    .bind(signature.to_string())
    .fetch_all(conn)
    .await?;
    Ok(pending)
}

pub async fn pending_txn_count(conn: &PgPool) -> anyhow::Result<usize> {
    // QUESTION: Pending Txns exists across two tables,
    // `pending_data_transfer_sessions` and `pending_txns`.
    // Do we want to be checking that both tables are empty?
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_txns")
        .fetch_one(conn)
        .await?;
    Ok(count as usize)
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

pub async fn save(
    txn: &mut Transaction<'_, Postgres>,
    req: &DataTransferSessionReq,
    last_timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    let dc_to_burn = bytes_to_dc(req.rewardable_bytes);

    sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
                uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
                downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
                rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
                last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
            "#
        )
            .bind(&req.data_transfer_usage.pub_key)
            .bind(&req.data_transfer_usage.payer)
            .bind(req.data_transfer_usage.upload_bytes as i64)
            .bind(req.data_transfer_usage.download_bytes as i64)
            .bind(req.rewardable_bytes as i64)
            .bind(last_timestamp)
            .execute(txn)
            .await?;

    increment_metric(&req.data_transfer_usage.payer, dc_to_burn);

    Ok(())
}

pub async fn delete_for_payer(
    conn: &Pool<Postgres>,
    payer: &PublicKeyBinary,
    burnt_dc: u64,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM data_transfer_sessions WHERE payer = $1")
        .bind(payer)
        .execute(conn)
        .await?;

    decrement_metric(payer, burnt_dc);

    Ok(())
}

pub async fn add_pending_transaction(
    conn: &PgPool,
    payer: &PublicKeyBinary,
    amount: u64,
    signature: &Signature,
) -> Result<(), sqlx::Error> {
    do_add_pending_transaction(conn, payer, amount, signature, Utc::now()).await?;
    Ok(())
}

pub async fn do_add_pending_transaction(
    conn: &PgPool,
    payer: &PublicKeyBinary,
    amount: u64,
    signature: &Signature,
    time_of_submission: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut txn = conn.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO pending_txns (signature, payer, amount, time_of_submission)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(signature.to_string())
    .bind(payer)
    .bind(amount as i64)
    .bind(time_of_submission)
    .execute(&mut *txn)
    .await?;

    sqlx::query(
        r#"
        WITH moved_rows AS (
            DELETE FROM data_transfer_sessions
            WHERE payer = $1
            RETURNING *
        )
        INSERT INTO pending_data_transfer_sessions (
            pub_key, 
            payer, 
            uploaded_bytes, 
            downloaded_bytes, 
            rewardable_bytes,
            first_timestamp, 
            last_timestamp, 
            signature
        )
        SELECT 
            pub_key, 
            payer, 
            uploaded_bytes, 
            downloaded_bytes, 
            rewardable_bytes,
            first_timestamp, 
            last_timestamp, 
            $2 
        FROM moved_rows;
        "#,
    )
    .bind(payer)
    .bind(signature.to_string())
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;
    Ok(())
}

pub async fn remove_pending_transaction_failure(
    conn: &PgPool,
    signature: &Signature,
) -> Result<(), sqlx::Error> {
    let mut txn = conn.begin().await?;
    let pt = sqlx::query("DELETE FROM pending_txns WHERE signature = $1")
        .bind(signature.to_string())
        .execute(&mut *txn)
        .await?;

    let moved = sqlx::query(
        r#"
        WITH moved_rows AS (
            DELETE FROM pending_data_transfer_sessions
            WHERE signature = $1
            RETURNING *
        )
        INSERT INTO data_transfer_sessions (
            pub_key, 
            payer, 
            uploaded_bytes, 
            downloaded_bytes, 
            rewardable_bytes,
            first_timestamp, 
            last_timestamp
        )
        SELECT 
            pub_key, 
            payer, 
            uploaded_bytes, 
            downloaded_bytes, 
            rewardable_bytes,
            first_timestamp, 
            last_timestamp
        FROM moved_rows
        ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
        "#,
    )
    .bind(signature.to_string())
    .execute(&mut *txn)
    .await?;
    println!("pending_txs deleted: {}", pt.rows_affected());
    println!("rows moved back: {}", moved.rows_affected());

    txn.commit().await?;

    Ok(())
}

pub async fn remove_pending_transaction_success(
    conn: &PgPool,
    signature: &Signature,
) -> Result<(), sqlx::Error> {
    let mut txn = conn.begin().await?;
    sqlx::query("DELETE FROM pending_txns WHERE signature = $1")
        .bind(signature.to_string())
        .execute(&mut *txn)
        .await?;

    sqlx::query("DELETE FROM pending_data_transfer_sessions WHERE signature = $1")
        .bind(signature.to_string())
        .execute(&mut *txn)
        .await?;

    txn.commit().await?;
    Ok(())
}

pub async fn fetch_all_pending_txns(conn: &PgPool) -> Result<Vec<PendingTxn>, sqlx::Error> {
    sqlx::query_as("SELECT * from pending_txns")
        .fetch_all(conn)
        .await
}

#[derive(Debug)]
pub struct PendingTxn {
    pub signature: Signature,
    pub payer: PublicKeyBinary,
    pub amount: u64,
    pub time_of_submission: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for PendingTxn {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            payer: row.try_get("payer")?,
            amount: row.try_get::<i64, _>("amount")? as u64,
            time_of_submission: row.try_get("time_of_submission")?,
            signature: row
                .try_get::<String, _>("signature")?
                .parse()
                .map_err(|e| sqlx::Error::ColumnDecode {
                    index: "signature".to_string(),
                    source: Box::new(e),
                })?,
        })
    }
}

fn set_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).set(value as f64);
}

fn increment_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).increment(value as f64);
}

fn decrement_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).decrement(value as f64);
}

const BYTES_PER_DC: u64 = 20_000;

pub fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    bytes.div_ceil(BYTES_PER_DC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_dc() {
        assert_eq!(1, bytes_to_dc(1));
        assert_eq!(1, bytes_to_dc(20_000));
        assert_eq!(2, bytes_to_dc(20_001));
    }
}
