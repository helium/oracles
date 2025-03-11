use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    mobile_reward_share, GatewayReward as MobileGatewayReward, MobileRewardShare,
};
use reward_index::indexer::{handle_mobile_rewards, RewardType};
use sqlx::PgPool;

use crate::common;

#[sqlx::test]
async fn accumulates_rewards(pool: PgPool) -> anyhow::Result<()> {
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

    let reward_shares = common::bytes_mut_stream(vec![
        make_gateway_reward(vec![1], 1),
        make_gateway_reward(vec![1], 2),
        make_gateway_reward(vec![1], 3),
    ]);

    let mut txn = pool.begin().await?;
    let manifest_time = Utc::now();
    handle_mobile_rewards(&mut txn, reward_shares, "unallocated-key", &manifest_time).await?;
    txn.commit().await?;

    let key = PublicKeyBinary::from(vec![1]);
    let reward = common::get_reward(&pool, &key.to_string(), RewardType::MobileGateway).await?;
    assert_eq!(reward.rewards, 6);

    Ok(())
}
