use crate::common::{self, reward_info_24_hours, AsStringKeyedMap};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::poc_mobile::UnallocatedRewardType, Message};
use mobile_verifier::{
    reward_shares, rewarder,
    subscriber_mapping_activity::{self, SubscriberMappingActivity},
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use std::{str::FromStr, string::ToString};

const SUBSCRIBER_1: &str = "subscriber1";
const SUBSCRIBER_2: &str = "subscriber2";
const SUBSCRIBER_3: &str = "subscriber3";
const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

#[sqlx::test]
async fn test_mapper_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();
    let reward_info = reward_info_24_hours();

    // seed db
    let mut txn = pool.clone().begin().await?;
    seed_mapping_data(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await.expect("db txn failed");

    rewarder::reward_mappers(&pool, mobile_rewards_client, &reward_info).await?;

    let rewards = mobile_rewards.finish().await?;
    let subscriber_rewards = rewards.subscriber_rewards.as_keyed_map();

    // assert the mapper rewards
    // all 3 subscribers will have an equal share,
    // requirement is 1 qualifying mapping criteria report per epoch
    // subscriber 1 has two qualifying mapping criteria reports,
    // other two subscribers one qualifying mapping criteria reports
    println!("{subscriber_rewards:?}");
    let sub_reward_1 = subscriber_rewards.get(SUBSCRIBER_1).expect("sub 1");
    assert_eq!(5_479_452_054_794, sub_reward_1.discovery_location_amount);

    let sub_reward_2 = subscriber_rewards.get(SUBSCRIBER_2).expect("sub 2");
    assert_eq!(5_479_452_054_794, sub_reward_2.discovery_location_amount);

    let sub_reward_3 = subscriber_rewards.get(SUBSCRIBER_3).expect("sub 3");
    assert_eq!(5_479_452_054_794, sub_reward_3.discovery_location_amount);

    // confirm our unallocated amount
    let unallocated_reward = rewards.unallocated.first().expect("unallocated");
    assert_eq!(
        UnallocatedRewardType::Mapper as i32,
        unallocated_reward.reward_type
    );
    assert_eq!(1, unallocated_reward.amount);

    // confirm the total rewards allocated matches expectations
    let expected_sum = reward_shares::get_scheduled_tokens_for_mappers(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    let subscriber_sum =
        rewards.total_sub_discovery_amount() + rewards.unallocated_amount_or_default();
    assert_eq!(expected_sum, subscriber_sum);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(subscriber_sum) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.2));

    Ok(())
}

#[sqlx::test]
async fn reward_mapper_check_entity_key_db(pool: PgPool) {
    let reward_info = reward_info_24_hours();
    // seed db
    let mut txn = pool.clone().begin().await.unwrap();
    seed_mapping_data(reward_info.epoch_period.end, &mut txn)
        .await
        .unwrap();
    txn.commit().await.expect("db txn failed");

    let rewardable_mapping_activity = subscriber_mapping_activity::db::rewardable_mapping_activity(
        &pool,
        &reward_info.epoch_period,
    )
    .await
    .unwrap();

    let sub_map = rewardable_mapping_activity.as_keyed_map();
    let sub_1 = sub_map.get(SUBSCRIBER_1).expect("sub 1");
    let sub_3 = sub_map.get(SUBSCRIBER_3).expect("sub 3");

    assert!(sub_1.reward_override_entity_key.is_none());
    assert_eq!(
        sub_3.reward_override_entity_key,
        Some("entity key".to_string())
    );
}

async fn seed_mapping_data(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // subscriber 1 has two qualifying mapping criteria reports
    // subscribers 2 and 3 have a single qualifying mapping criteria report

    let reports = vec![
        SubscriberMappingActivity {
            received_timestamp: ts - ChronoDuration::hours(1),
            subscriber_id: SUBSCRIBER_1.to_string().encode_to_vec(),
            discovery_reward_shares: 30,
            verification_reward_shares: 0,
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
            reward_override_entity_key: None,
        },
        SubscriberMappingActivity {
            received_timestamp: ts - ChronoDuration::hours(2),
            subscriber_id: SUBSCRIBER_1.to_string().encode_to_vec(),
            discovery_reward_shares: 30,
            verification_reward_shares: 0,
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
            reward_override_entity_key: None,
        },
        SubscriberMappingActivity {
            received_timestamp: ts - ChronoDuration::hours(1),
            subscriber_id: SUBSCRIBER_2.to_string().encode_to_vec(),
            discovery_reward_shares: 30,
            verification_reward_shares: 0,
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
            reward_override_entity_key: None,
        },
        SubscriberMappingActivity {
            received_timestamp: ts - ChronoDuration::hours(1),
            subscriber_id: SUBSCRIBER_3.to_string().encode_to_vec(),
            discovery_reward_shares: 30,
            verification_reward_shares: 0,
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
            reward_override_entity_key: Some("entity key".to_string()),
        },
    ];

    subscriber_mapping_activity::db::save(txn, stream::iter(reports.into_iter()).map(anyhow::Ok))
        .await?;

    Ok(())
}
