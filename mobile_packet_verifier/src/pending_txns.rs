use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use solana::Signature;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

use crate::pending_burns::DataTransferSession;

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

pub async fn add_pending_txn(
    conn: &PgPool,
    payer: &PublicKeyBinary,
    amount: u64,
    signature: &Signature,
) -> Result<(), sqlx::Error> {
    do_add_pending_txn(conn, payer, amount, signature, Utc::now()).await?;
    Ok(())
}

pub async fn do_add_pending_txn(
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

pub async fn remove_pending_txn_failure(
    conn: &PgPool,
    signature: &Signature,
) -> Result<(), sqlx::Error> {
    let mut txn = conn.begin().await?;
    sqlx::query("DELETE FROM pending_txns WHERE signature = $1")
        .bind(signature.to_string())
        .execute(&mut *txn)
        .await?;

    sqlx::query(
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

    txn.commit().await?;

    Ok(())
}

pub async fn remove_pending_txn_success(
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
