use chrono::{DateTime, Duration, DurationRound, Utc};
use file_store::BytesMutStream;
use futures::{stream, StreamExt};
use prost::bytes::{Bytes, BytesMut};
use reward_index::indexer::RewardType;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

pub fn bytes_mut_stream<T: prost::Message + Send + 'static>(els: Vec<T>) -> BytesMutStream {
    BytesMutStream::from(
        stream::iter(els)
            .map(|el| Bytes::from(el.encode_to_vec()))
            .map(|el| BytesMut::from(el.as_ref()))
            .map(Ok)
            .boxed(),
    )
}

// When retrieving a timestamp from DB, depending on the version of postgres
// the timestamp may be truncated. When comparing datetimes, to ones generated
// in a test with `Utc::now()`, you should truncate it.
pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbReward {
    pub address: String,
    pub rewards: u64,
    pub last_reward: DateTime<Utc>,
    pub reward_type: RewardType,
}

impl FromRow<'_, PgRow> for DbReward {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            address: row.get("address"),
            rewards: row.get::<i64, _>("rewards") as u64,
            last_reward: row.try_get("last_reward")?,
            reward_type: row.try_get("reward_type")?,
        })
    }
}

pub async fn get_reward(
    pool: &PgPool,
    key: &str,
    reward_type: RewardType,
) -> anyhow::Result<DbReward> {
    let reward: DbReward = sqlx::query_as(
        r#"
            SELECT *
            FROM reward_index 
            WHERE address = $1 AND reward_type = $2
            "#,
    )
    .bind(key)
    .bind(reward_type)
    .fetch_one(pool)
    .await?;

    Ok(reward)
}
