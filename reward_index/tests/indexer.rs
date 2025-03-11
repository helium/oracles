use chrono::{DateTime, Utc};
use file_store::{traits::MsgBytes, BytesMutStream};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    poc_lora::{iot_reward_share, GatewayReward as IotGatewayReward, IotRewardShare},
    poc_mobile::{mobile_reward_share, GatewayReward as MobileGatewayReward, MobileRewardShare},
};
use prost::bytes::BytesMut;
use reward_index::indexer::{handle_iot_rewards, handle_mobile_rewards, RewardType};
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

fn bytes_mut_stream<T: MsgBytes + Send + 'static>(els: Vec<T>) -> BytesMutStream {
    BytesMutStream::from(
        stream::iter(els.into_iter())
            .map(|el| el.as_bytes())
            .map(|el| BytesMut::from(el.as_ref()))
            .map(Ok)
            .boxed(),
    )
}

#[sqlx::test]
async fn test_mobile_indexer(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(hotspot_key: Vec<u8>, dc_transfer_reward: u64) -> MobileRewardShare {
        MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(mobile_reward_share::Reward::GatewayReward(
                MobileGatewayReward {
                    hotspot_key,
                    dc_transfer_reward,
                    rewardable_bytes: 0,
                    price: 0,
                },
            )),
        }
    }

    let reward_shares = bytes_mut_stream(vec![
        make_gateway_reward(vec![1], 1),
        make_gateway_reward(vec![1], 2),
        make_gateway_reward(vec![1], 3),
    ]);

    let mut txn = pool.begin().await?;
    let manifest_time = Utc::now();
    handle_mobile_rewards(&mut txn, reward_shares, "unallocated-key", &manifest_time).await?;
    txn.commit().await?;

    let key = PublicKeyBinary::from(vec![1]);
    let reward = get(&pool, &key, RewardType::MobileGateway).await?;
    assert_eq!(reward.rewards, 6);

    Ok(())
}
#[sqlx::test]
async fn test_iot_indexer(pool: PgPool) -> anyhow::Result<()> {
    fn make_gateway_reward(
        hotspot_key: Vec<u8>,
        beacon_amount: u64,
        witness_amount: u64,
        dc_transfer_amount: u64,
    ) -> IotRewardShare {
        IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(iot_reward_share::Reward::GatewayReward(IotGatewayReward {
                hotspot_key,
                beacon_amount,
                witness_amount,
                dc_transfer_amount,
            })),
        }
    }

    let reward_shares = bytes_mut_stream(vec![
        make_gateway_reward(vec![1], 1, 2, 3),
        make_gateway_reward(vec![1], 4, 5, 6),
        make_gateway_reward(vec![1], 7, 8, 9),
    ]);

    let mut txn = pool.begin().await?;
    let manifest_time = Utc::now();
    handle_iot_rewards(
        &mut txn,
        reward_shares,
        "op-fund",
        "unallocated-key",
        &manifest_time,
    )
    .await?;
    txn.commit().await?;

    let key = PublicKeyBinary::from(vec![1]);
    let reward = get(&pool, &key, RewardType::IotGateway).await?;
    assert_eq!(reward.rewards, 45);

    Ok(())
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

pub async fn get(
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
