use std::ops::Range;

use chrono::{DateTime, Utc};
use file_store::unique_connections::UniqueConnectionsIngestReport;
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, Transaction};

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
            SELECT DISTINCT ON(hotspot_pubkey)
                hotspot_pubkey, unique_connections
            FROM unique_connections
            WHERE received_timestamp >= $1 AND received_timestamp < $2
            ORDER BY hotspot_pubkey, received_timestamp DESC
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
    reports: &[UniqueConnectionsIngestReport],
) -> Result<(), sqlx::Error> {
    const BATCH_SIZE: usize = (u16::MAX / 5) as usize;

    for chunk in reports.chunks(BATCH_SIZE) {
        QueryBuilder::new(
            r#"
            INSERT INTO unique_connections
            (hotspot_pubkey, unique_connections, start_timestamp, end_timestamp, received_timestamp)
            "#,
        )
        .push_values(chunk, |mut b, report| {
            b.push_bind(report.report.pubkey.to_string())
                .push_bind(report.report.unique_connections as i64)
                .push_bind(report.report.start_timestamp)
                .push_bind(report.report.end_timestamp)
                .push_bind(report.received_timestamp);
        })
        .push(
            r#"
            ON CONFLICT
                (hotspot_pubkey, received_timestamp)
                DO NOTHING
            "#,
        )
        .build()
        .execute(&mut **txn)
        .await?;
    }

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
    .execute(&mut **txn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use file_store::unique_connections::UniqueConnectionReq;
    use helium_crypto::{KeyTag, Keypair};
    use rand::rngs::OsRng;

    use super::*;

    #[sqlx::test]
    fn only_use_latest_within_window(pool: PgPool) -> anyhow::Result<()> {
        // In the case connection counts need to be reprocessed,
        // make sure we grab only the latest count for a radio
        // when there may be more than one row for a radio in the window.

        let keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
        let pubkey_bin: PublicKeyBinary = keypair.public_key().to_owned().into();

        let now = Utc::now();

        let base_report = UniqueConnectionsIngestReport {
            received_timestamp: Utc::now(),
            report: UniqueConnectionReq {
                pubkey: pubkey_bin.clone(),
                start_timestamp: now - chrono::Duration::days(7),
                end_timestamp: now,
                unique_connections: 0,
                timestamp: now,
                carrier_key: pubkey_bin.clone(),
                signature: vec![],
            },
        };

        // Prepare two reports for the same window.
        // Both will be saved, but only the connection count in the second should be used.
        let first = now - chrono::Duration::hours(5);
        let second = now - chrono::Duration::hours(2);

        let report_one = UniqueConnectionsIngestReport {
            received_timestamp: first,
            report: UniqueConnectionReq {
                unique_connections: 2,
                ..base_report.report.clone()
            },
        };
        let report_two = UniqueConnectionsIngestReport {
            received_timestamp: second,
            report: UniqueConnectionReq {
                unique_connections: 1,
                ..report_one.report.clone()
            },
        };

        let mut txn = pool.begin().await?;
        save(&mut txn, &[report_one, report_two]).await?;
        txn.commit().await?;

        let epoch = (now - chrono::Duration::days(1))..now;
        let uniq_conns = get(&pool, &epoch).await?;
        let conns = uniq_conns.get(&pubkey_bin).cloned().unwrap();
        assert_eq!(1, conns);

        Ok(())
    }
}
