use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use solana::Signature;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

use crate::pending_burns::{self, DataTransferSession};

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
    do_add_pending_txn(conn, payer, amount, signature, Utc::now()).await
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

    // Move pending data sessions back to the main table
    let transfer_sessions: Vec<DataTransferSession> = sqlx::query_as(
        r#"
        DELETE FROM pending_data_transfer_sessions
        WHERE signature = $1
        RETURNING *
        "#,
    )
    .bind(signature.to_string())
    .fetch_all(&mut *txn)
    .await?;

    for session in transfer_sessions.iter() {
        pending_burns::save_data_transfer_session(&mut txn, session).await?;
    }

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
