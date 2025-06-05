use std::ops::Range;

use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt};
use sqlx::{PgExecutor, Postgres, QueryBuilder, Transaction};

use crate::subscriber_mapping_activity::SubscriberMappingActivity;

use super::SubscriberMappingShares;

pub async fn save(
    transaction: &mut Transaction<'_, Postgres>,
    ingest_reports: impl Stream<Item = anyhow::Result<SubscriberMappingActivity>>,
) -> anyhow::Result<()> {
    const NUM_IN_BATCH: usize = (u16::MAX / 6) as usize;

    ingest_reports
        .try_chunks(NUM_IN_BATCH)
        .err_into::<anyhow::Error>()
        .try_fold(transaction, |txn, chunk| async move {
            QueryBuilder::new(r#"INSERT INTO subscriber_mapping_activity(
                    subscriber_id, discovery_reward_shares, verification_reward_shares, received_timestamp, inserted_at, reward_override_entity_key)"#)
            .push_values(chunk, |mut b, activity| {

                b.push_bind(activity.subscriber_id)
                    .push_bind(activity.discovery_reward_shares as i64)
                    .push_bind(activity.verification_reward_shares as i64)
                    .push_bind(activity.received_timestamp)
                    .push_bind(Utc::now())
                    .push_bind(activity.reward_override_entity_key);
            })
            .push("ON CONFLICT (subscriber_id, received_timestamp) DO NOTHING")
            .build()
            .execute(&mut **txn)
            .await?;

            Ok(txn)
        })
        .await?;

    Ok(())
}

pub async fn rewardable_mapping_activity(
    db: impl PgExecutor<'_>,
    epoch_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<Vec<SubscriberMappingShares>> {
    sqlx::query_as(
        r#"
        SELECT DISTINCT ON (subscriber_id) subscriber_id, discovery_reward_shares, verification_reward_shares, reward_override_entity_key
        FROM subscriber_mapping_activity
        WHERE received_timestamp >= $1
            AND received_timestamp < $2
            AND (discovery_reward_shares > 0 OR verification_reward_shares > 0)
        ORDER BY subscriber_id, received_timestamp DESC
        "#,
    )
    .bind(epoch_period.start)
    .bind(epoch_period.end)
    .fetch_all(db)
    .await
    .map_err(anyhow::Error::from)
}

pub async fn clear(db: impl PgExecutor<'_>, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
    sqlx::query(
        "
        DELETE FROM subscriber_mapping_activity
        WHERE received_timestamp < $1
    ",
    )
    .bind(timestamp)
    .execute(db)
    .await?;

    Ok(())
}
