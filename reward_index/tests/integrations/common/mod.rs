use chrono::{DateTime, Utc};
use file_store::{traits::MsgBytes, BytesMutStream};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use prost::bytes::BytesMut;
use reward_index::indexer::RewardType;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

pub fn bytes_mut_stream<T: MsgBytes + Send + 'static>(els: Vec<T>) -> BytesMutStream {
    BytesMutStream::from(
        stream::iter(els.into_iter())
            .map(|el| el.as_bytes())
            .map(|el| BytesMut::from(el.as_ref()))
            .map(Ok)
            .boxed(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewardIndex {
    pub address: PublicKeyBinary,
    pub rewards: u64,
    pub last_reward: DateTime<Utc>,
    pub reward_type: RewardType,
}

impl FromRow<'_, PgRow> for RewardIndex {
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
    key: &PublicKeyBinary,
    reward_type: RewardType,
) -> anyhow::Result<RewardIndex> {
    let reward: RewardIndex = sqlx::query_as(
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
