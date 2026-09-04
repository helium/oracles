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

/// The sessions behind an in-flight burn, grouped for the burned record.
///
/// Grouped on the multiplier stored on each row when the burn was submitted,
/// not on a fresh read of the ticket history. The amount is already fixed on
/// chain, so the records have to reproduce the groups it came from. Asking the
/// history again could give a different answer if a backdated ticket arrived
/// since, and the record would then disagree with the charge.
pub async fn get_pending_data_sessions_for_signature(
    conn: &PgPool,
    signature: &Signature,
) -> anyhow::Result<Vec<DataTransferSession>> {
    let pending: Vec<DataTransferSession> = sqlx::query_as(
        r#"
        SELECT * FROM pending_data_transfer_sessions
        WHERE signature = $1
        "#,
    )
    .bind(signature.to_string())
    .fetch_all(conn)
    .await?;

    Ok(pending_burns::group_by_multiplier(&pending))
}

pub async fn pending_txn_count(conn: &PgPool) -> anyhow::Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_txns")
        .fetch_one(conn)
        .await?;
    Ok(count as usize)
}

/// `priced` is the per-file rows as [`pending_burns::get_all_payer_burns`]
/// priced them, not the groups it billed, and `amount` is their total.
///
/// They are passed in rather than looked up again so that the multiplier a row
/// moves with is the one its share of `amount` came from, even if a ticket
/// arrives between the pricing and this call.
pub async fn add_pending_txn(
    conn: &PgPool,
    payer: &PublicKeyBinary,
    amount: u64,
    signature: &Signature,
    priced: &[DataTransferSession],
) -> Result<(), sqlx::Error> {
    do_add_pending_txn(conn, payer, amount, signature, priced, Utc::now()).await
}

pub async fn do_add_pending_txn(
    conn: &PgPool,
    payer: &PublicKeyBinary,
    amount: u64,
    signature: &Signature,
    priced: &[DataTransferSession],
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

    // The multiplier comes from `priced`, not from the row, because
    // `data_transfer_sessions` has no such column.
    //
    // Matched on (pub_key, last_timestamp), which is what identifies a row: one
    // hotspot in one file.
    //
    // The fallback to 1 covers a row written between the pricing read and this
    // call. The daemon does not ingest while a burn is running, so that should
    // not happen; see the note in PLAN.md for what would go wrong if it did.
    sqlx::query(
        r#"
        WITH priced AS (
            SELECT * FROM UNNEST($3::text[], $4::timestamptz[], $5::numeric[])
                AS t(pub_key, last_timestamp, multiplier)
        ), moved_rows AS (
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
            multiplier,
            first_timestamp,
            last_timestamp,
            signature
        )
        SELECT
            moved_rows.pub_key,
            moved_rows.payer,
            moved_rows.uploaded_bytes,
            moved_rows.downloaded_bytes,
            moved_rows.rewardable_bytes,
            COALESCE(priced.multiplier, 1),
            moved_rows.first_timestamp,
            moved_rows.last_timestamp,
            $2
        FROM moved_rows
        LEFT JOIN priced
            ON priced.pub_key = moved_rows.pub_key
           AND priced.last_timestamp = moved_rows.last_timestamp;
        "#,
    )
    .bind(payer)
    .bind(signature.to_string())
    .bind(
        priced
            .iter()
            .map(|s| s.pub_key().to_string())
            .collect::<Vec<_>>(),
    )
    .bind(
        priced
            .iter()
            .map(|s| s.last_timestamp())
            .collect::<Vec<_>>(),
    )
    .bind(priced.iter().map(|s| s.multiplier()).collect::<Vec<_>>())
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;
    Ok(())
}

pub async fn remove_pending_txn_failure(
    conn: &PgPool,
    signature: &Signature,
) -> anyhow::Result<()> {
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

    pending_burns::save_data_transfer_sessions(&mut txn, &transfer_sessions).await?;

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
