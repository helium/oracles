use chrono::Utc;
use futures::{Stream, TryStreamExt};
use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::subscriber_mapping_activity::SubscriberMappingActivity;

pub async fn save(
    transaction: &mut Transaction<'_, Postgres>,
    ingest_reports: impl Stream<Item = anyhow::Result<SubscriberMappingActivity>>,
) -> anyhow::Result<()> {
    const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;

    ingest_reports
        .try_chunks(NUM_IN_BATCH)
        .err_into::<anyhow::Error>()
        .try_fold(transaction, |txn, chunk| async move {
            QueryBuilder::new("INSERT INTO subscriber_mapping_activity(subscriber_id, discovery_reward_shares, verification_reward_shares, received_timestamp, inserted_at)")
            .push_values(chunk, |mut b, activity| {

                b.push_bind(activity.subscriber_id)
                    .push_bind(activity.discovery_reward_shares as i64)
                    .push_bind(activity.verification_reward_shares as i64)
                    .push_bind(activity.received_timestamp)
                    .push_bind(Utc::now());
            })
            .build()
            .execute(&mut *txn)
            .await?;

            Ok(txn)
        })
        .await?;

    Ok(())
}
