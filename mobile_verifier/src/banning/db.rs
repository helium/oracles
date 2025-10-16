use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use file_store_helium_proto::mobile_ban::{BanAction, BanType, VerifiedBanReport};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use sqlx::{PgConnection, PgPool, Row};

use super::BAN_CLEANUP_DAYS;

// When retreiving banned radios, we want to get the
// latest ban for a radio at the given time.
//
// If a radio was banned for POC yesterday,
// and today a ban for DATA comes in.
// Running files from yesterday, the radio should not be banned.
//
// When dealing with epoch dates
// 00:00:00 -> 23:59:59
//
// `received_timestamp`:
// The ban must have been received _before_ the start of the epoch.
// `<` exclusive less than
//
// `expiration_timestamp`:
// Expiration must be throughout the entire duration of the epoch.
// `>=` inclusive greater than
pub async fn get_banned_radios(
    pool: &PgPool,
    epoch_end: DateTime<Utc>,
) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    let banned = sqlx::query(
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (hotspot_pubkey) *
            FROM hotspot_bans
            WHERE
                received_timestamp < $1
                AND (expiration_timestamp IS NULL
                    OR expiration_timestamp >= $1)
            ORDER BY hotspot_pubkey, received_timestamp DESC
        )
        SELECT hotspot_pubkey
        FROM latest
        WHERE ban_type IN ('all', 'poc')
        "#,
    )
    .bind(epoch_end)
    .fetch(pool)
    .map_ok(|row| row.get("hotspot_pubkey"))
    .try_collect()
    .await?;

    Ok(banned)
}

pub(super) async fn clear_bans(
    txn: &mut PgConnection,
    before: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM hotspot_bans WHERE expiration_timestamp < $1")
        .bind(before - Duration::days(BAN_CLEANUP_DAYS))
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
        BanAction::Unban(_details) => {
            expire_previous_bans(
                conn,
                ban_report.hotspot_pubkey(),
                ban_report.report.received_timestamp,
            )
            .await?
        }
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
        ON CONFLICT (hotspot_pubkey, received_timestamp)
        DO NOTHING
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

async fn expire_previous_bans(
    conn: &mut PgConnection,
    hotspot_pubkey: &PublicKeyBinary,
    received_timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    let res = sqlx::query(
        r#"
        UPDATE hotspot_bans
        SET expiration_timestamp = $1
        WHERE hotspot_pubkey = $2
        "#,
    )
    .bind(received_timestamp)
    .bind(hotspot_pubkey)
    .execute(conn)
    .await?;

    tracing::info!(
        %hotspot_pubkey,
        count = res.rows_affected(),
        "expired bans"
    );

    Ok(())
}
