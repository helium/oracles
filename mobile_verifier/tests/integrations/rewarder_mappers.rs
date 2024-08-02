use crate::common::{self, MockFileSinkReceiver};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use file_store::mobile_subscriber::{SubscriberLocationIngestReport, SubscriberLocationReq};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{SubscriberReward, UnallocatedReward, UnallocatedRewardType},
    Message,
};
use mobile_verifier::{reward_shares, rewarder, subscriber_location};
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
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    // seed db
    let mut txn = pool.clone().begin().await?;
    seed_mapping_data(epoch.end, &mut txn).await?;
    txn.commit().await.expect("db txn failed");

    let (_, rewards) = tokio::join!(
        rewarder::reward_mappers(&pool, &mobile_rewards_client, &epoch),
        receive_expected_rewards(&mut mobile_rewards)
    );

    if let Ok((subscriber_rewards, unallocated_reward)) = rewards {
        // assert the mapper rewards
        // all 3 subscribers will have an equal share,
        // requirement is 1 qualifying mapping criteria report per epoch
        // subscriber 1 has two qualifying mapping criteria reports,
        // other two subscribers one qualifying mapping criteria reports
        assert_eq!(
            SUBSCRIBER_1.to_string().encode_to_vec(),
            subscriber_rewards[0].subscriber_id
        );
        assert_eq!(
            5_479_452_054_794,
            subscriber_rewards[0].discovery_location_amount
        );

        assert_eq!(
            SUBSCRIBER_2.to_string().encode_to_vec(),
            subscriber_rewards[1].subscriber_id
        );
        assert_eq!(
            5_479_452_054_794,
            subscriber_rewards[2].discovery_location_amount
        );

        assert_eq!(
            SUBSCRIBER_3.to_string().encode_to_vec(),
            subscriber_rewards[2].subscriber_id
        );
        assert_eq!(
            5_479_452_054_794,
            subscriber_rewards[2].discovery_location_amount
        );

        // confirm our unallocated amount
        assert_eq!(
            UnallocatedRewardType::Mapper as i32,
            unallocated_reward.reward_type
        );
        assert_eq!(1, unallocated_reward.amount);

        // confirm the total rewards allocated matches expectations
        let expected_sum = reward_shares::get_scheduled_tokens_for_mappers(epoch.end - epoch.start)
            .to_u64()
            .unwrap();
        let subscriber_sum = subscriber_rewards[0].discovery_location_amount
            + subscriber_rewards[1].discovery_location_amount
            + subscriber_rewards[2].discovery_location_amount
            + unallocated_reward.amount;
        assert_eq!(expected_sum, subscriber_sum);

        // confirm the rewarded percentage amount matches expectations
        let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
        let percent = (Decimal::from(subscriber_sum) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(percent, dec!(0.2));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

async fn receive_expected_rewards(
    mobile_rewards: &mut MockFileSinkReceiver,
) -> anyhow::Result<(Vec<SubscriberReward>, UnallocatedReward)> {
    // get the filestore outputs from rewards run
    // we will have 3 radio rewards, 1 wifi radio and 2 cbrs radios
    let subscriber_reward1 = mobile_rewards.receive_subscriber_reward().await;
    let subscriber_reward2 = mobile_rewards.receive_subscriber_reward().await;
    let subscriber_reward3 = mobile_rewards.receive_subscriber_reward().await;
    let mut subscriber_rewards = vec![subscriber_reward1, subscriber_reward2, subscriber_reward3];

    subscriber_rewards.sort_by(|a, b| a.subscriber_id.cmp(&b.subscriber_id));

    // expect one unallocated reward
    let unallocated_reward = mobile_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    mobile_rewards.assert_no_messages();

    Ok((subscriber_rewards, unallocated_reward))
}

async fn seed_mapping_data(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // subscriber 1 has two qualifying mapping criteria reports
    // subscribers 2 and 3 have a single qualifying mapping criteria report

    let report1 = SubscriberLocationIngestReport {
        received_timestamp: ts - ChronoDuration::hours(1),
        report: SubscriberLocationReq {
            subscriber_id: SUBSCRIBER_1.to_string().encode_to_vec(),
            timestamp: ts - ChronoDuration::hours(1),
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
        },
    };
    let report2 = SubscriberLocationIngestReport {
        received_timestamp: ts - ChronoDuration::hours(1),
        report: SubscriberLocationReq {
            subscriber_id: SUBSCRIBER_1.to_string().encode_to_vec(),
            timestamp: ts - ChronoDuration::hours(2),
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
        },
    };
    let report3 = SubscriberLocationIngestReport {
        received_timestamp: ts - ChronoDuration::hours(1),
        report: SubscriberLocationReq {
            subscriber_id: SUBSCRIBER_2.to_string().encode_to_vec(),
            timestamp: ts - ChronoDuration::hours(3),
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
        },
    };
    let report4 = SubscriberLocationIngestReport {
        received_timestamp: ts - ChronoDuration::hours(1),
        report: SubscriberLocationReq {
            subscriber_id: SUBSCRIBER_3.to_string().encode_to_vec(),
            timestamp: ts - ChronoDuration::hours(3),
            carrier_pub_key: PublicKeyBinary::from_str(HOTSPOT_1).unwrap(),
        },
    };
    subscriber_location::save(&report1, txn).await?;
    subscriber_location::save(&report2, txn).await?;
    subscriber_location::save(&report3, txn).await?;
    subscriber_location::save(&report4, txn).await?;

    Ok(())
}
