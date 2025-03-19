use chrono::{DateTime, Duration, DurationRound, Utc};
use file_store::{traits::MsgBytes, BytesMutStream};
use futures::{stream, StreamExt};
use helium_proto::services::{poc_lora::IotRewardShare, poc_mobile::MobileRewardShare};
use prost::bytes::BytesMut;
use reward_index::indexer::RewardType;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

// mobile and iot reward stream helpers are kept separate so you can create
// an empty stream without needing to provide the type in the test.

pub fn mobile_rewards_stream(els: Vec<MobileRewardShare>) -> BytesMutStream {
    BytesMutStream::from(
        stream::iter(els)
            .map(|el| el.as_bytes())
            .map(|el| BytesMut::from(el.as_ref()))
            .map(Ok)
            .boxed(),
    )
}

pub fn iot_rewards_stream(els: Vec<IotRewardShare>) -> BytesMutStream {
    BytesMutStream::from(
        stream::iter(els)
            .map(|el| el.as_bytes())
            .map(|el| BytesMut::from(el.as_ref()))
            .map(Ok)
            .boxed(),
    )
}

// When retreiving a timestamp from DB, depending on the version of postgres
// the timestamp may be truncated. When comparing datetimes, to ones generated
// in a test with `Utc::now()`, you should truncate it.
pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbReward {
    pub address: String,
    pub rewards: u64,
    pub claimable: u64,
    pub last_reward: DateTime<Utc>,
    pub reward_type: RewardType,
}

impl FromRow<'_, PgRow> for DbReward {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            address: row.get("address"),
            rewards: row.get::<i64, _>("rewards") as u64,
            claimable: row.get::<i64, _>("claimable") as u64,
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
