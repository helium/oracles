use std::ops::Range;

use chrono::{DateTime, Utc};
use file_store::unique_connections::UniqueConnectionsIngestReport;
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use super::UniqueConnectionCounts;

pub async fn get(
    db: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<UniqueConnectionCounts> {
    #[derive(FromRow)]
    struct UniqueConnections {
        hotspot_pubkey: PublicKeyBinary,
        #[sqlx(try_from = "i64")]
        unique_connections: u64,
    }

    let rows = sqlx::query_as::<_, UniqueConnections>(
        r#"
            SELECT hotspot_pubkey, unique_connections
            FROM unique_connections
            WHERE received_timestamp >= $1 AND received_timestamp <= $2
            ORDER BY received_timestamp DESC
            "#,
    )
    .bind(reward_period.start)
    .bind(reward_period.end)
    .fetch(db)
    .and_then(|row| async move { Ok((row.hotspot_pubkey, row.unique_connections)) })
    .try_collect()
    .await?;

    Ok(rows)
}

pub async fn save(
    txn: &mut Transaction<'_, Postgres>,
    report: &UniqueConnectionsIngestReport,
) -> Result<(), sqlx::Error> {
    // TODO: on conflict?
    sqlx::query(
        r#"
        INSERT INTO unique_connections 
        (hotspot_pubkey, unique_connections, start_timestamp, end_timestamp, received_timestamp)
        VALUES
        ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(report.report.pubkey.to_string())
    .bind(report.report.unique_connections as i64)
    .bind(report.report.start_timestamp)
    .bind(report.report.end_timestamp)
    .bind(report.received_timestamp)
    .execute(txn)
    .await?;

    Ok(())
}

pub async fn clear(
    txn: &mut Transaction<'_, Postgres>,
    timestamp: &DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        DELETE FROM unique_connections
        WHERE received_timestamp < $1
        "#,
    )
    .bind(timestamp)
    .execute(txn)
    .await?;
    Ok(())
}
