use chrono::{Duration, Utc};
use helium_crypto::PublicKeyBinary;
use reward_index::{db, indexer::handle_mobile_rewards};
use sqlx::PgPool;

use helium_proto::services::poc_mobile::{mobile_reward_share, GatewayReward, MobileRewardShare};

use crate::common::mobile_rewards_stream;

#[sqlx::test]
async fn migrate_known_radios(pool: PgPool) -> anyhow::Result<()> {
    let keys = vec![vec![1], vec![2], vec![3], vec![4], vec![5], vec![6]];

    let rewards = mobile_rewards_stream(
        keys.iter()
            .map(|key| make_gateway_reward(key.clone(), 1))
            .collect::<Vec<_>>(),
    );

    let mut txn = pool.begin().await?;
    handle_mobile_rewards(&mut txn, rewards, &Utc::now(), "unalloc-key").await?;
    txn.commit().await?;

    for key in keys.iter() {
        let k = PublicKeyBinary::from(key.clone()).to_string();
        let dur = db::escrow_duration::get(&pool, &k).await?;
        assert_eq!(None, dur);
    }

    let expires_on = Utc::now() + Duration::days(90);

    db::escrow_duration::migrate_known_radios(&pool, expires_on.date_naive()).await?;

    for key in keys {
        let k = PublicKeyBinary::from(key.clone()).to_string();
        let days = db::escrow_duration::get(&pool, &k).await?;
        assert_eq!(Some((0, Some(expires_on.date_naive()))), days);
    }

    Ok(())
}

#[sqlx::test]
async fn get_escrow_duration(pool: PgPool) -> anyhow::Result<()> {
    let today = Utc::now().date_naive();

    let _ = db::escrow_duration::insert(&pool, "one", 42, Some(Utc::now().date_naive())).await?;
    let dur = db::escrow_duration::get(&pool, "one").await?;
    assert_eq!(Some((42, Some(today))), dur);

    let _ = db::escrow_duration::insert(&pool, "two", 42, None).await?;
    let dur = db::escrow_duration::get(&pool, "two").await?;
    assert_eq!(Some((42, None)), dur);

    Ok(())
}

#[sqlx::test]
async fn update_escrow_duration(pool: PgPool) -> anyhow::Result<()> {
    let today = Utc::now().date_naive();
    let key = "one";

    let _ = db::escrow_duration::insert(&pool, key, 42, Some(Utc::now().date_naive())).await?;
    let dur = db::escrow_duration::get(&pool, key).await?;
    assert_eq!(Some((42, Some(today))), dur);

    let _ = db::escrow_duration::insert(&pool, key, 42, None).await?;
    let dur = db::escrow_duration::get(&pool, key).await?;
    assert_eq!(Some((42, None)), dur);

    Ok(())
}

#[sqlx::test]
async fn delete_escrow_duration(pool: PgPool) -> anyhow::Result<()> {
    let key = "one";

    let _ = db::escrow_duration::insert(&pool, key, 42, Some(Utc::now().date_naive())).await?;
    let dur = db::escrow_duration::get(&pool, key).await?;
    assert_eq!(Some((42, Some(Utc::now().date_naive()))), dur);

    db::escrow_duration::delete(&pool, key).await?;
    let dur = db::escrow_duration::get(&pool, key).await?;
    assert_eq!(None, dur);

    Ok(())
}

fn make_gateway_reward(hotspot_key: Vec<u8>, dc_transfer_reward: u64) -> MobileRewardShare {
    MobileRewardShare {
        start_period: Utc::now().timestamp_millis() as u64,
        end_period: Utc::now().timestamp_millis() as u64,
        reward: Some(mobile_reward_share::Reward::GatewayReward(GatewayReward {
            hotspot_key,
            dc_transfer_reward,
            rewardable_bytes: 0,
            price: 0,
        })),
    }
}
