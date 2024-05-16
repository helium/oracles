use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use sqlx::{Pool, Postgres};

const LAST_REWARD_PROCESSED_TIME: &str = "last_reward_processed_time";

pub async fn initialize(db: &Pool<Postgres>) -> anyhow::Result<()> {
    match meta::fetch(db, LAST_REWARD_PROCESSED_TIME).await {
        Ok(timestamp) => last_reward_processed_time(db, to_datetime(timestamp)?).await,
        Err(db_store::Error::NotFound(_)) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub async fn last_reward_processed_time(
    db: &Pool<Postgres>,
    datetime: DateTime<Utc>,
) -> anyhow::Result<()> {
    metrics::gauge!(LAST_REWARD_PROCESSED_TIME).set(datetime.timestamp() as f64);
    meta::store(db, LAST_REWARD_PROCESSED_TIME, datetime.timestamp()).await?;

    Ok(())
}

fn to_datetime(timestamp: i64) -> anyhow::Result<DateTime<Utc>> {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("Unable to decode timestamp"))
}
