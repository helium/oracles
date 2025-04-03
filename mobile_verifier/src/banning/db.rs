use std::collections::HashSet;

use chrono::{DateTime, Utc};
use file_store::mobile_ban::{BanAction, BanType, VerifiedBanReport};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};

pub async fn get_banned_radios(
    pool: &PgPool,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    let banned = sqlx::query(
        r#"
            SELECT hotspot_pubkey
            FROM hotspot_bans
            WHERE 
                ban_type in ('all', 'poc')
                AND received_timestamp <= $1
                AND (expiration_timestamp IS NULL 
                    OR expiration_timestamp >= $1)
            "#,
    )
    .bind(timestamp)
    .fetch(pool)
    .map_ok(|row| row.get("hotspot_pubkey"))
    .try_collect()
    .await?;

    Ok(banned)
}

pub async fn cleanup_bans(
    txn: &mut Transaction<'_, Postgres>,
    before: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM hotspot_bans WHERE expiration_timestamp < $1")
        .bind(before)
        .execute(txn)
        .await?;

    Ok(())
}

pub async fn update_hotspot_ban(
    conn: &mut PgConnection,
    ban_report: &VerifiedBanReport,
) -> anyhow::Result<()> {
    match &ban_report.report.report.ban_action {
        BanAction::Ban(details) => {
            insert_ban(
                conn,
                ban_report.hotspot_pubkey(),
                ban_report.report.received_timestamp,
                details.expiration_timestamp,
                details.ban_type,
            )
            .await?
        }
        BanAction::Unban(_details) => remove_ban(conn, ban_report.hotspot_pubkey()).await?,
    }

    Ok(())
}

async fn insert_ban(
    conn: &mut PgConnection,
    hotspot_pubkey: &PublicKeyBinary,
    received_timestamp: DateTime<Utc>,
    expiration_timestamp: Option<DateTime<Utc>>,
    ban_type: BanType,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
            INSERT INTO hotspot_bans
                (hotspot_pubkey, received_timestamp, expiration_timestamp, ban_type)
            VALUES
                ($1, $2, $3, $4)
            ON CONFLICT (hotspot_pubkey) DO UPDATE SET
                received_timestamp = EXCLUDED.received_timestamp,
                expiration_timestamp = EXCLUDED.expiration_timestamp,
                ban_type = EXCLUDED.ban_type
            "#,
    )
    .bind(hotspot_pubkey)
    .bind(received_timestamp)
    .bind(expiration_timestamp)
    .bind(ban_type.as_str_name())
    .execute(conn)
    .await?;

    Ok(())
}

async fn remove_ban(
    conn: &mut PgConnection,
    hotspot_pubkey: &PublicKeyBinary,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM hotspot_bans WHERE hotspot_pubkey = $1")
        .bind(hotspot_pubkey)
        .execute(conn)
        .await?;

    Ok(())
}
