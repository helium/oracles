use crate::common;

use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use reward_index::indexer::{handle_iot_rewards, RewardType};
use sqlx::PgPool;

use helium_proto::services::poc_lora::{
    iot_reward_share, GatewayReward as IotGatewayReward, IotRewardShare,
};

#[sqlx::test]
async fn accumulates_rewards(pool: PgPool) -> anyhow::Result<()> {
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

    let reward_shares = common::bytes_mut_stream(vec![
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
    let reward = common::get_reward(&pool, &key, RewardType::IotGateway).await?;
    assert_eq!(reward.rewards, 45);

    Ok(())
}
