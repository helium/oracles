use crate::common::{self, MockFileSinkReceiver, MockHexBoostingClient, RadioRewardV2Ext};
use chrono::{DateTime, Duration as ChronoDuration, Duration, Utc};
use file_store::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    mobile_radio_threshold::{RadioThresholdIngestReport, RadioThresholdReportReq},
    speedtest::CellSpeedtest,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    poc_lora::UnallocatedRewardType,
    poc_mobile::{
        CoverageObjectValidity, HeartbeatValidity, LocationSource, MobileRewardShare,
        RadioRewardV2, SeniorityUpdateReason, SignalLevel, UnallocatedReward,
    },
};
use hextree::Cell;
use mobile_config::boosted_hex_info::{BoostedHexDeviceType, BoostedHexInfo};
use mobile_verifier::{
    cell_type::CellType,
    coverage::CoverageObject,
    heartbeats::{HbType, Heartbeat, ValidatedHeartbeat},
    radio_threshold, reward_shares, rewarder, speedtests,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use solana_sdk::pubkey::Pubkey;
use sqlx::{PgPool, Postgres, Transaction};
use std::{num::NonZeroU32, str::FromStr};
use uuid::Uuid;

const HOTSPOT_1: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const HOTSPOT_2: &str = "112QhnxqU8QZ3jUXpoRk51quuQVft9Pf5P5zzDDvLxj7Q9QqbMh7";
const HOTSPOT_3: &str = "11hd7HoicRgBPjBGcqcT2Y9hRQovdZeff5eKFMbCSuDYQmuCiF1";
const HOTSPOT_4: &str = "11fEisW6J38vnS6qL65QyxnnNV5jfukFhuFiD4uteo4eUgDSShK";
const CARRIER_HOTSPOT_KEY: &str = "11hd7HoicRgBPjBGcqcT2Y9hRQovdZeff5eKFMbCSuDYQmuCiF1";
const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
const BOOST_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

async fn update_assignments(pool: &PgPool) -> anyhow::Result<()> {
    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_default(),
    )
    .await?;
    Ok(())
}

//
// TODO: add a bootstrapper to reduce boiler plate
//

#[sqlx::test]
async fn test_poc_with_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let epoch_duration = epoch.end - epoch.start;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have a coverage reports for a singluar hex location per radio
    seed_heartbeats_v1(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = epoch.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where no start or end time is set
    // will default to the first multiplier
    // first multiplier is 1x for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's location
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let total_poc_emissions = reward_shares::get_scheduled_tokens_for_poc(epoch_duration)
        .to_u64()
        .unwrap();

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards_maybe_unallocated(
            &mut mobile_rewards,
            ExpectUnallocated::NoWhenValue(total_poc_emissions)
        )
    );

    let Ok((poc_rewards, unallocated_reward)) = rewards else {
        panic!("no rewards received");
    };

    let mut poc_rewards = poc_rewards.iter();
    let hotspot_2 = poc_rewards.next().unwrap();
    let hotspot_1 = poc_rewards.next().unwrap();
    let hotspot_3 = poc_rewards.next().unwrap();
    assert_eq!(
        None,
        poc_rewards.next(),
        "Received more hotspots than expected in rewards"
    );
    assert_eq!(
        HOTSPOT_2.to_string(),
        PublicKeyBinary::from(hotspot_2.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_1.to_string(),
        PublicKeyBinary::from(hotspot_1.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_3.to_string(),
        PublicKeyBinary::from(hotspot_3.hotspot_key.clone()).to_string()
    );

    // Calculating expected rewards
    let (regular_poc, boosted_poc) = get_poc_allocation_buckets(epoch_duration);

    // With regular poc now 50% of total emissions, that will be split
    // between the 3 radios equally. 900 comes from IndoorWifi 400 *
    // 0.75 speedtest multiplier * 3 radios
    let regular_share = regular_poc / dec!(900);

    // Boosted hexes are 10x and 20x.
    // (300 * 19) + (300 * 9) = 8400;
    // To get points _only_ from boosting.
    let boosted_share = boosted_poc / dec!(8400);

    let exp_reward_1 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(19));
    let exp_reward_2 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(9));
    let exp_reward_3 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(0));

    assert_eq!(exp_reward_1, hotspot_2.total_poc_reward()); // 20x boost
    assert_eq!(exp_reward_2, hotspot_1.total_poc_reward()); // 10x boost
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward()); // no boost

    // assert the boosted hexes in the radio rewards
    // assert the number of boosted hexes for each radio
    assert_eq!(1, hotspot_2.boosted_hexes_len());
    assert_eq!(1, hotspot_1.boosted_hexes_len());
    // hotspot 3 has 1 boosted hex at 1x, it does not effect rewards, but all
    // covered hexes are reported with their corresponding boost values.
    assert_eq!(1, hotspot_3.boosted_hexes_len());

    // assert the hex boost multiplier values
    assert_eq!(20, hotspot_2.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(10, hotspot_1.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(1, hotspot_3.nth_boosted_hex(0).boosted_multiplier);

    // assert the hex boost location values
    assert_eq!(0x8a1fb49642dffff_u64, hotspot_2.nth_boosted_hex(0).location);
    assert_eq!(0x8a1fb466d2dffff_u64, hotspot_1.nth_boosted_hex(0).location);

    // confirm the total rewards allocated matches expectations
    let poc_sum =
        hotspot_1.total_poc_reward() + hotspot_2.total_poc_reward() + hotspot_3.total_poc_reward();
    let total = poc_sum + unallocated_reward.amount;
    assert_eq!(total_poc_emissions, total);

    // confirm the rewarded percentage amount matches expectations
    let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
    let percent = (Decimal::from(total) / daily_total)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

#[sqlx::test]
async fn test_poc_boosted_hexes_thresholds_not_met(pool: PgPool) -> anyhow::Result<()> {
    // this is the same setup as the previous one, but with the hotspot thresholds not seeded
    // this simulates the case where we have radios in boosted hexes but where the coverage
    // thresholds for the radios have not been met
    // the end result is that no boosting takes place, the radios are awarded non boosted reward values
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have a coverage reports for a singluar hex location per radio
    seed_heartbeats_v1(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = epoch.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where no start or end time is set
    // will default to the first multiplier
    // first multiplier is 1x for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's location
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards(&mut mobile_rewards)
    );
    if let Ok((poc_rewards, unallocated_reward)) = rewards {
        // assert poc reward outputs
        let exp_reward_1 = 16438356164383;
        let exp_reward_2 = 16438356164383;
        let exp_reward_3 = 16438356164383;

        assert_eq!(exp_reward_1, poc_rewards[0].total_poc_reward());
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].total_poc_reward());
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].total_poc_reward());
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        assert_eq!(0, poc_rewards[0].boosted_hexes_len());
        assert_eq!(0, poc_rewards[1].boosted_hexes_len());
        assert_eq!(0, poc_rewards[2].boosted_hexes_len());

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();
        let unallocated_sum: u64 = unallocated_reward.amount;
        let total = poc_sum + unallocated_sum;

        let expected_sum = reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
            .to_u64()
            .unwrap();
        assert_eq!(expected_sum, total);

        // confirm the rewarded percentage amount matches expectations
        let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
        let percent = (Decimal::from(total) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(percent, dec!(0.6));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

#[sqlx::test]
async fn test_poc_with_multi_coverage_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let epoch_duration = epoch.end - epoch.start;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have multiple coverage reports for one radio and one report for the others
    seed_heartbeats_v2(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = epoch.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let start_ts_2 = epoch.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where reward start time is in the first period length
    // default to 1x multiplier for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let start_ts_3 = epoch.start;
    let end_ts_3 = start_ts_3 + (boost_period_length * multipliers3.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's first covered location
            location: Cell::from_raw(0x8a1fb46622dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1.clone(),
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 1's second covered location
            location: Cell::from_raw(0x8a1fb46622d7fff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: Some(start_ts_3),
            end_ts: Some(end_ts_3),
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let total_poc_emissions = reward_shares::get_scheduled_tokens_for_poc(epoch_duration)
        .to_u64()
        .unwrap();

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);
    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards_maybe_unallocated(
            &mut mobile_rewards,
            ExpectUnallocated::NoWhenValue(total_poc_emissions)
        )
    );

    let Ok((poc_rewards, unallocated_reward)) = rewards else {
        panic!("no rewards received");
    };

    let mut poc_rewards = poc_rewards.iter();
    let hotspot_2 = poc_rewards.next().unwrap(); // 1 boost at 20x
    let hotspot_1 = poc_rewards.next().unwrap(); // 2 boost at 10x
    let hotspot_3 = poc_rewards.next().unwrap(); // no boost
    assert_eq!(
        None,
        poc_rewards.next(),
        "Received more hotspots than expected in rewards"
    );

    assert_eq!(
        HOTSPOT_2.to_string(),
        PublicKeyBinary::from(hotspot_2.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_1.to_string(),
        PublicKeyBinary::from(hotspot_1.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_3.to_string(),
        PublicKeyBinary::from(hotspot_3.hotspot_key.clone()).to_string()
    );

    // Calculating expected rewards
    // - 2 covered hexes boosted at 10x
    // - 1 covered hex boosted at 20x
    // - 1 covered hex no boost
    let (regular_poc, boosted_poc) = get_poc_allocation_buckets(epoch_duration);

    // With regular poc now 50% of total emissions, that will be split
    // between the 3 radios equally.
    // 1200 comes from IndoorWifi 400 * 0.75 speedtest multiplier * 4 hexes
    let regular_share = regular_poc / dec!(1200);

    // Boosted hexes are 2 at 10x and 1 at 20x.
    // (300 * (9 * 2)) + (300 * 19) = 11,100;
    // To get points _only_ from boosting.
    let boosted_share = boosted_poc / dec!(11_100);

    let hex_coverage = |hexes: u8| regular_share * dec!(300) * Decimal::from(hexes);
    let boost_coverage = |mult: u8| boosted_share * dec!(300) * Decimal::from(mult);

    let exp_reward_1 = rounded(hex_coverage(2)) + rounded(boost_coverage(18));
    let exp_reward_2 = rounded(hex_coverage(1)) + rounded(boost_coverage(19));
    let exp_reward_3 = rounded(hex_coverage(1)) + rounded(boost_coverage(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward()); // 2 at 10x boost
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward()); // 1 at 20x boost
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward()); // 1 at no boost

    // hotspot 1 and 2 should have the same coverage points, but different poc rewards.
    assert_eq!(
        hotspot_1.total_coverage_points(),
        hotspot_2.total_coverage_points()
    );
    assert_ne!(hotspot_1.total_poc_reward(), hotspot_2.total_poc_reward());

    // assert the number of boosted hexes for each radio
    assert_eq!(1, hotspot_2.boosted_hexes_len());
    assert_eq!(2, hotspot_1.boosted_hexes_len());
    // hotspot 3 has 1 boosted hex at 1x, it does not effect rewards, but all
    // covered hexes are reported with their corresponding boost values.
    assert_eq!(1, hotspot_3.boosted_hexes_len());

    // assert the hex boost multiplier values
    // as hotspot 3 has 2 covered hexes, it should have 2 boosted hexes
    // sort order in the vec for these is not guaranteed, so sort them
    let mut hotspot_1_boosted_hexes = hotspot_1.boosted_hexes();
    hotspot_1_boosted_hexes.sort_by(|a, b| b.location.cmp(&a.location));

    assert_eq!(20, hotspot_2.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(10, hotspot_1_boosted_hexes[0].boosted_multiplier);
    assert_eq!(10, hotspot_1_boosted_hexes[1].boosted_multiplier);

    // assert the hex boost location values
    assert_eq!(0x8a1fb46622dffff_u64, hotspot_1_boosted_hexes[0].location);
    assert_eq!(0x8a1fb46622d7fff_u64, hotspot_1_boosted_hexes[1].location);
    assert_eq!(0x8a1fb49642dffff_u64, hotspot_2.nth_boosted_hex(0).location);

    // confirm the total rewards allocated matches expectations
    let poc_sum =
        hotspot_1.total_poc_reward() + hotspot_2.total_poc_reward() + hotspot_3.total_poc_reward();
    let total = poc_sum + unallocated_reward.amount;
    assert_eq!(total_poc_emissions, total);

    // confirm the rewarded percentage amount matches expectations
    let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
    let percent = (Decimal::from(total) / daily_total)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

#[sqlx::test]
async fn test_expired_boosted_hex(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_v1(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is after the boost period ends
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
    ];
    let start_ts_1 =
        epoch.start - (boost_period_length * multipliers1.len() as i32 + ChronoDuration::days(1));
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is same as the boost period ends
    let multipliers2 = vec![
        NonZeroU32::new(4).unwrap(),
        NonZeroU32::new(12).unwrap(),
        NonZeroU32::new(17).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * multipliers2.len() as i32);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards(&mut mobile_rewards)
    );
    if let Ok((poc_rewards, unallocated_reward)) = rewards {
        // assert poc reward outputs
        let exp_reward_1 = 16_438_356_164_383;
        let exp_reward_2 = 16_438_356_164_383;
        let exp_reward_3 = 16_438_356_164_383;

        assert_eq!(exp_reward_1, poc_rewards[0].total_poc_reward());
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].total_poc_reward());
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].total_poc_reward());
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        // all will be zero as the boost period has expired for the single boosted hex
        assert_eq!(0, poc_rewards[0].boosted_hexes_len());
        assert_eq!(0, poc_rewards[1].boosted_hexes_len());
        assert_eq!(0, poc_rewards[2].boosted_hexes_len());

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();
        let unallocated_sum: u64 = unallocated_reward.amount;
        let total = poc_sum + unallocated_sum;

        let expected_sum = reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
            .to_u64()
            .unwrap();
        assert_eq!(expected_sum, total);

        // confirm the rewarded percentage amount matches expectations
        let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
        let percent = (Decimal::from(total) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(percent, dec!(0.6));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

#[sqlx::test]
async fn test_reduced_location_score_with_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let epoch_duration = epoch.end - epoch.start;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_with_location_trust(
        epoch.start,
        &mut txn,
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        HotspotLocationTrust {
            meters: 300,
            multiplier: dec!(0.25),
        },
    )
    .await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![NonZeroU32::new(2).unwrap()];
    let start_ts_1 = epoch.start;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where no start or end time is set
    let multipliers2 = vec![NonZeroU32::new(2).unwrap()];

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's location
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);
    let total_poc_emissions = reward_shares::get_scheduled_tokens_for_poc(epoch_duration)
        .to_u64()
        .unwrap();

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards_maybe_unallocated(
            &mut mobile_rewards,
            ExpectUnallocated::NoWhenValue(total_poc_emissions)
        )
    );

    let Ok((poc_rewards, unallocated_reward)) = rewards else {
        panic!("no rewards received");
    };

    let mut poc_rewards = poc_rewards.iter();
    let hotspot_2 = poc_rewards.next().unwrap(); // full location trust NO boosts
    let hotspot_1 = poc_rewards.next().unwrap(); // full location trust 1 boost
    let hotspot_3 = poc_rewards.next().unwrap(); // reduced location trust 1 boost
    assert_eq!(
        None,
        poc_rewards.next(),
        "Received more hotspots than expected in rewards"
    );
    assert_eq!(
        HOTSPOT_1.to_string(),
        PublicKeyBinary::from(hotspot_1.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_2.to_string(),
        PublicKeyBinary::from(hotspot_2.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_3.to_string(),
        PublicKeyBinary::from(hotspot_3.hotspot_key.clone()).to_string()
    );

    // Calculating expected rewards
    let (regular_poc, boosted_poc) = get_poc_allocation_buckets(epoch_duration);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 0.25     | 75    |
    // |---------------------|-----------|----------|-------|
    //                                              | 675   |
    let regular_share = regular_poc / dec!(675);

    // Boosted hexes are 2x, only one radio qualifies based on the location trust
    // 300 * 1 == 300
    // To get points _only_ from boosting.
    let boosted_share = boosted_poc / dec!(300);

    let exp_reward_1 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(1));
    let exp_reward_2 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(0));
    let exp_reward_3 =
        rounded(regular_share * dec!(75)) + rounded(boosted_share * dec!(75) * dec!(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward());
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward());
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward());

    // assert the number of boosted hexes for each radio
    //hotspot 1 has one boosted hex
    assert_eq!(1, hotspot_1.boosted_hexes_len());
    //hotspot 2 has no boosted hexes
    assert_eq!(0, hotspot_2.boosted_hexes_len());
    // hotspot 3 has a boosted location but as its location trust score
    // is reduced the boost does not get applied
    assert_eq!(0, hotspot_3.boosted_hexes_len());

    // assert the hex boost multiplier values
    // assert_eq!(2, hotspot_1.boosted_hexes[0].multiplier);
    assert_eq!(2, hotspot_1.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(0x8a1fb466d2dffff_u64, hotspot_1.nth_boosted_hex(0).location);

    // confirm the total rewards allocated matches expectations
    let poc_sum =
        hotspot_1.total_poc_reward() + hotspot_2.total_poc_reward() + hotspot_3.total_poc_reward();
    let total = poc_sum + unallocated_reward.amount;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    // confirm the rewarded percentage amount matches expectations
    let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
    let percent = (Decimal::from(total) / daily_total)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

#[sqlx::test]
async fn test_distance_from_asserted_removes_boosting_but_not_location_trust(
    pool: PgPool,
) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let epoch_duration = epoch.end - epoch.start;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_with_location_trust(
        epoch.start,
        &mut txn,
        // hotspot 1 can receive boosting
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        // hotspot 2 can receive boosting but has no boosted hexes
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        // hotspot 3 is too far for boosting
        HotspotLocationTrust {
            meters: 100,
            multiplier: dec!(1.0),
        },
    )
    .await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![NonZeroU32::new(2).unwrap()];
    let start_ts_1 = epoch.start;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where no start or end time is set
    let multipliers2 = vec![NonZeroU32::new(2).unwrap()];

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's location
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);
    let total_poc_emissions = reward_shares::get_scheduled_tokens_for_poc(epoch_duration)
        .to_u64()
        .unwrap();

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards_maybe_unallocated(
            &mut mobile_rewards,
            ExpectUnallocated::NoWhenValue(total_poc_emissions)
        )
    );

    let Ok((poc_rewards, unallocated_reward)) = rewards else {
        panic!("no rewards received");
    };

    let mut poc_rewards = poc_rewards.iter();
    let hotspot_2 = poc_rewards.next().unwrap(); // full location trust NO boosts
    let hotspot_1 = poc_rewards.next().unwrap(); // full location trust 1 boost
    let hotspot_3 = poc_rewards.next().unwrap(); // reduced location trust 1 boost
    assert_eq!(
        None,
        poc_rewards.next(),
        "Received more hotspots than expected in rewards"
    );
    assert_eq!(
        HOTSPOT_1.to_string(),
        PublicKeyBinary::from(hotspot_1.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_2.to_string(),
        PublicKeyBinary::from(hotspot_2.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_3.to_string(),
        PublicKeyBinary::from(hotspot_3.hotspot_key.clone()).to_string()
    );

    // Calculating expected rewards
    let (regular_poc, boosted_poc) = get_poc_allocation_buckets(epoch_duration);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // |---------------------|-----------|----------|-------|
    //                                              | 900   |
    let regular_share = regular_poc / dec!(900);

    // Boosted hexes are 2x, only one radio qualifies based on the location trust
    // 300 * 1 == 300
    // To get points _only_ from boosting.
    let boosted_share = boosted_poc / dec!(300);

    let exp_reward_1 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(1));
    let exp_reward_2 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(0));
    let exp_reward_3 =
        rounded(regular_share * dec!(300)) + rounded(boosted_share * dec!(300) * dec!(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward());
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward());
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward());

    // assert the number of boosted hexes for each radio
    //hotspot 1 has one boosted hex
    assert_eq!(1, hotspot_1.boosted_hexes_len());
    //hotspot 2 has no boosted hexes
    assert_eq!(0, hotspot_2.boosted_hexes_len());
    // hotspot 3 has a boosted location but as its location trust score
    // is reduced the boost does not get applied
    assert_eq!(0, hotspot_3.boosted_hexes_len());

    // assert the hex boost multiplier values
    // assert_eq!(2, hotspot_1.boosted_hexes[0].multiplier);
    assert_eq!(2, hotspot_1.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(0x8a1fb466d2dffff_u64, hotspot_1.nth_boosted_hex(0).location);

    // confirm the total rewards allocated matches expectations
    let poc_sum =
        hotspot_1.total_poc_reward() + hotspot_2.total_poc_reward() + hotspot_3.total_poc_reward();
    let total = poc_sum + unallocated_reward.amount;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    // confirm the rewarded percentage amount matches expectations
    let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
    let percent = (Decimal::from(total) / daily_total)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

#[sqlx::test]
async fn test_poc_with_cbrs_and_multi_coverage_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let epoch_duration = epoch.end - epoch.start;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have multiple coverage reports for one radio and one report for the others
    // include a cbrs radio alongside 2 wifi radios
    seed_heartbeats_v4(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_radio_thresholds(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = epoch.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where reward start time is in the first period length
    // default to 1x multiplier for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_3 = epoch.start;
    let end_ts_3 = start_ts_3 + (boost_period_length * multipliers3.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's first covered location
            location: Cell::from_raw(0x8a1fb46622dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1.clone(),
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 1's second covered location
            location: Cell::from_raw(0x8a1fb46622d7fff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: Some(start_ts_3),
            end_ts: Some(end_ts_3),
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
            device_type: BoostedHexDeviceType::All,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);
    let total_poc_emissions = reward_shares::get_scheduled_tokens_for_poc(epoch_duration)
        .to_u64()
        .unwrap();

    let (_, rewards) = tokio::join!(
        // run rewards for poc and dc
        rewarder::reward_poc_and_dc(
            &pool,
            &hex_boosting_client,
            &mobile_rewards_client,
            &speedtest_avg_client,
            &epoch,
            dec!(0.0001)
        ),
        receive_expected_rewards_maybe_unallocated(
            &mut mobile_rewards,
            ExpectUnallocated::NoWhenValue(total_poc_emissions)
        )
    );
    let Ok((poc_rewards, unallocated_reward)) = rewards else {
        panic!("no rewards received");
    };

    let mut poc_rewards = poc_rewards.iter();
    let hotspot_2 = poc_rewards.next().unwrap(); // 1 boost at 20x
    let hotspot_1 = poc_rewards.next().unwrap(); // 2 boosts at 10x
    let hotspot_3 = poc_rewards.next().unwrap(); // no boosts
    assert_eq!(
        None,
        poc_rewards.next(),
        "Received more hotspots than expected in rewards"
    );
    assert_eq!(
        HOTSPOT_2.to_string(),
        PublicKeyBinary::from(hotspot_2.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_1.to_string(),
        PublicKeyBinary::from(hotspot_1.hotspot_key.clone()).to_string()
    );
    assert_eq!(
        HOTSPOT_4.to_string(),
        PublicKeyBinary::from(hotspot_3.hotspot_key.clone()).to_string()
    );

    // Calculating expected rewards
    let (regular_poc, boosted_poc) = get_poc_allocation_buckets(epoch_duration);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 400 x 2             | 0.75      | 1.00     | 600   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 100                 | 0.75      | 1.00     | 75    |
    // |---------------------|-----------|----------|-------|
    //                                              | 975   |
    let regular_share = regular_poc / dec!(975);

    // Boosted hexes are 2 at 10x and 1 at 20x.
    // Only wifi is targeted with Boosts.
    // (300 * (9 * 2)) + (300 * 19) == 11,100
    // To get points _only_ from boosting.
    let boosted_share = boosted_poc / dec!(11_100);

    let exp_reward_1 = rounded(regular_share * dec!(300) * dec!(2))
        + rounded(boosted_share * dec!(300) * dec!(18));
    let exp_reward_2 = rounded(regular_share * dec!(300) * dec!(1))
        + rounded(boosted_share * dec!(300) * dec!(19));
    let exp_reward_3 =
        rounded(regular_share * dec!(75) * dec!(1)) + rounded(boosted_share * dec!(75) * dec!(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward());
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward());
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward());

    // assert the number of boosted hexes for each radio
    assert_eq!(1, hotspot_2.boosted_hexes_len());
    assert_eq!(2, hotspot_1.boosted_hexes_len());
    // hotspot 3 has 1 boosted hex at 1x, it does not effect rewards, but all
    // covered hexes are reported with their corresponding boost values.
    assert_eq!(1, hotspot_3.boosted_hexes_len());

    // assert the hex boost multiplier values
    // as hotspot 3 has 2 covered hexes, it should have 2 boosted hexes
    // sort order in the vec for these is not guaranteed, so sort them
    let mut hotspot_1_boosted_hexes = hotspot_1.boosted_hexes();
    hotspot_1_boosted_hexes.sort_by(|a, b| b.location.cmp(&a.location));

    assert_eq!(20, hotspot_2.nth_boosted_hex(0).boosted_multiplier);
    assert_eq!(10, hotspot_1_boosted_hexes[1].boosted_multiplier);
    assert_eq!(10, hotspot_1_boosted_hexes[1].boosted_multiplier);

    // assert the hex boost location values
    assert_eq!(0x8a1fb46622dffff_u64, hotspot_1_boosted_hexes[0].location);
    assert_eq!(0x8a1fb46622d7fff_u64, hotspot_1_boosted_hexes[1].location);
    assert_eq!(0x8a1fb49642dffff_u64, hotspot_2.nth_boosted_hex(0).location);

    // confirm the total rewards allocated matches expectations
    let poc_sum =
        hotspot_1.total_poc_reward() + hotspot_2.total_poc_reward() + hotspot_3.total_poc_reward();
    let total = poc_sum + unallocated_reward.amount;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    // confirm the rewarded percentage amount matches expectations
    let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
    let percent = (Decimal::from(total) / daily_total)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

fn rounded(num: Decimal) -> u64 {
    num.to_u64().unwrap_or_default()
}

#[sqlx::test]
async fn test_poc_boosted_hex_stack_multiplier(pool: PgPool) -> anyhow::Result<()> {
    // This test seeds the database with 2 identically performing radios on
    // neighboring hexes. Both are eligible for boosted rewards.
    //
    // In the first pass, no hexes are boosted, to ensure the radios are equal.
    // In the second pass, 1 radio is boosted with 2 boosts.
    //  - legacy boost,   BoostType::All
    //  - specific boost, BoostType::CbrsIndoor
    // Rewards are checked to ensure the 2 boosts accumulate correctly.

    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    let boosted_pubkey = PublicKeyBinary::from_str(HOTSPOT_1)?;
    let unboosted_pubkey = PublicKeyBinary::from_str(HOTSPOT_2)?;

    let boosted_location = Cell::from_raw(0x8a1fb466d2dffff)?;
    let unboosted_location = Cell::from_raw(0x8a1fb46622d7fff)?;

    let boostable_radios = vec![
        HexBoostableRadio::cbrs_indoor(boosted_pubkey, boosted_location),
        HexBoostableRadio::cbrs_indoor(unboosted_pubkey, unboosted_location),
    ];

    let mut txn = pool.begin().await?;
    for radio in boostable_radios {
        radio.seed(epoch.start, &mut txn).await?;
    }
    txn.commit().await?;
    update_assignments(&pool).await?;

    // First pass with no boosted hexes, radios perform the same
    {
        let hex_boosting_client = MockHexBoostingClient::new(vec![]);
        let (_, poc_rewards) = tokio::join!(
            rewarder::reward_poc_and_dc(
                &pool,
                &hex_boosting_client,
                &mobile_rewards_client,
                &speedtest_avg_client,
                &epoch,
                dec!(0.0001)
            ),
            async {
                let one = mobile_rewards.receive_radio_reward().await;
                let two = mobile_rewards.receive_radio_reward().await;

                mobile_rewards.assert_no_messages();

                [one, two]
            },
        );

        let [radio_one, radio_two] = &poc_rewards;
        assert_eq!(
            radio_one.coverage_points, radio_two.coverage_points,
            "radios perform equally unboosted"
        );
        assert_eq!(
            radio_one.poc_reward, radio_two.poc_reward,
            "radios earn equally unboosted"
        );

        // Make sure there were no unallocated rewards
        let total = poc_rewards.iter().map(|r| r.poc_reward).sum::<u64>();
        assert_eq!(
            reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
                .to_u64()
                .unwrap(),
            total,
            "allocated equals scheduled output"
        );
    }

    // Second pass with boosted hex, first radio will be setup to receive 10x boost
    {
        let base = BoostedHexInfo {
            location: boosted_location,
            start_ts: None,
            end_ts: None,
            period_length: Duration::days(30),
            multipliers: vec![],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY)?,
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY)?,
            version: 0,
            device_type: BoostedHexDeviceType::All,
        };

        let boosted_hexes = vec![
            BoostedHexInfo {
                multipliers: vec![NonZeroU32::new(3).unwrap()],
                device_type: BoostedHexDeviceType::All,
                ..base.clone()
            },
            BoostedHexInfo {
                multipliers: vec![NonZeroU32::new(7).unwrap()],
                device_type: BoostedHexDeviceType::CbrsIndoor,
                ..base.clone()
            },
        ];

        let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);
        let (_, mut poc_rewards) = tokio::join!(
            rewarder::reward_poc_and_dc(
                &pool,
                &hex_boosting_client,
                &mobile_rewards_client,
                &speedtest_avg_client,
                &epoch,
                dec!(0.0001)
            ),
            async move {
                let one = mobile_rewards.receive_radio_reward().await;
                let two = mobile_rewards.receive_radio_reward().await;

                mobile_rewards.assert_no_messages();

                [one, two]
            },
        );

        // sort by rewards ascending
        poc_rewards.sort_by_key(|r| r.poc_reward);
        let [unboosted, boosted] = &poc_rewards;

        let boosted_hex = boosted.boosted_hexes.first().expect("boosted hex");
        assert_eq!(10, boosted_hex.multiplier);

        assert_eq!(
            10,
            boosted.coverage_points / unboosted.coverage_points,
            "boosted radio should have 10x coverage_points"
        );
        assert_eq!(
            10,
            boosted.poc_reward / unboosted.poc_reward,
            "boosted radio should have 10x poc_rewards"
        );

        // Make sure there were no unallocated rewards
        let total = poc_rewards.iter().map(|r| r.poc_reward).sum::<u64>();
        assert_eq!(
            reward_shares::get_scheduled_tokens_for_poc(epoch.end - epoch.start)
                .to_u64()
                .unwrap(),
            total,
            "allocated equals scheduled output"
        );
    }

    Ok(())
}

async fn receive_expected_rewards(
    mobile_rewards: &mut MockFileSinkReceiver<MobileRewardShare>,
) -> anyhow::Result<(Vec<RadioRewardV2>, UnallocatedReward)> {
    receive_expected_rewards_maybe_unallocated(mobile_rewards, ExpectUnallocated::Yes).await
}

enum ExpectUnallocated {
    Yes,
    NoWhenValue(u64),
}

async fn receive_expected_rewards_maybe_unallocated(
    mobile_rewards: &mut MockFileSinkReceiver<MobileRewardShare>,
    expect_unallocated: ExpectUnallocated,
) -> anyhow::Result<(Vec<RadioRewardV2>, UnallocatedReward)> {
    // get the filestore outputs from rewards run
    let radio_reward1 = mobile_rewards.receive_radio_reward().await;
    let radio_reward2 = mobile_rewards.receive_radio_reward().await;
    let radio_reward3 = mobile_rewards.receive_radio_reward().await;

    // ordering is not guaranteed, so stick the rewards into a vec and sort
    let mut poc_rewards = vec![radio_reward1, radio_reward2, radio_reward3];
    poc_rewards.sort_by(|a, b| b.hotspot_key.cmp(&a.hotspot_key));

    let unallocated_poc_reward = match expect_unallocated {
        ExpectUnallocated::Yes => mobile_rewards.receive_unallocated_reward().await,
        ExpectUnallocated::NoWhenValue(max_emission) => {
            let total: u64 = poc_rewards.iter().map(|p| p.total_poc_reward()).sum();
            let emitted_is_total = total == max_emission;
            tracing::info!(
                emitted_is_total,
                total,
                max_emission,
                "receiving expected rewards unallocated amount"
            );
            if emitted_is_total {
                UnallocatedReward {
                    reward_type: UnallocatedRewardType::Poc.into(),
                    amount: 0,
                }
            } else {
                mobile_rewards.receive_unallocated_reward().await
            }
        }
    };

    // should be no further msgs
    mobile_rewards.assert_no_messages();

    Ok((poc_rewards, unallocated_poc_reward))
}

async fn seed_heartbeats_v1(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    seed_heartbeats_with_location_trust(
        ts,
        txn,
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
        HotspotLocationTrust {
            meters: 10,
            multiplier: dec!(1.0),
        },
    )
    .await?;

    Ok(())
}

async fn seed_heartbeats_v2(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();

        let cov_obj_1 = create_multi_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key1.clone(),
            vec![0x8a1fb46622dffff_u64, 0x8a1fb46622d7fff_u64],
            true,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key3: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap();
        let cov_obj_3 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key3.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key3,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat1, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat2, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat3, txn).await?;

        wifi_heartbeat1.save(txn).await?;
        wifi_heartbeat2.save(txn).await?;
        wifi_heartbeat3.save(txn).await?;

        cov_obj_1.save(txn).await?;
        cov_obj_2.save(txn).await?;
        cov_obj_3.save(txn).await?;
    }
    Ok(())
}

struct HotspotLocationTrust {
    meters: i64,
    multiplier: Decimal,
}

async fn seed_heartbeats_with_location_trust(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
    hs_1_location: HotspotLocationTrust,
    hs_2_location: HotspotLocationTrust,
    hs_3_location: HotspotLocationTrust,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();
        let cov_obj_1 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key1.clone(),
            0x8a1fb466d2dffff_u64,
            true,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(hs_1_location.meters),
            coverage_meta: None,
            location_trust_score_multiplier: hs_1_location.multiplier,
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(hs_2_location.meters),
            coverage_meta: None,
            location_trust_score_multiplier: hs_2_location.multiplier,
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key3: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap();
        let cov_obj_3 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key3.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key3,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(hs_3_location.meters),
            coverage_meta: None,
            location_trust_score_multiplier: hs_3_location.multiplier,
            validity: HeartbeatValidity::Valid,
        };

        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat1, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat2, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat3, txn).await?;

        wifi_heartbeat1.save(txn).await?;
        wifi_heartbeat2.save(txn).await?;
        wifi_heartbeat3.save(txn).await?;

        cov_obj_1.save(txn).await?;
        cov_obj_2.save(txn).await?;
        cov_obj_3.save(txn).await?;
    }
    Ok(())
}

async fn seed_heartbeats_v4(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();

        let cov_obj_1 = create_multi_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key1.clone(),
            vec![0x8a1fb46622dffff_u64, 0x8a1fb46622d7fff_u64],
            true,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            None,
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key4: PublicKeyBinary = HOTSPOT_4.to_string().parse().unwrap();
        let cbsd_id = Some("P27-SCE4255W0002".to_string());
        let cov_obj_3 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            cbsd_id.clone(),
            hotspot_key4.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let cbrs_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Cbrs,
                hotspot_key: hotspot_key4,
                cbsd_id,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::SercommOutdoor,
            distance_to_asserted: Some(1),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat1, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat2, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &cbrs_heartbeat1, txn).await?;

        wifi_heartbeat1.save(txn).await?;
        wifi_heartbeat2.save(txn).await?;
        cbrs_heartbeat1.save(txn).await?;

        cov_obj_1.save(txn).await?;
        cov_obj_2.save(txn).await?;
        cov_obj_3.save(txn).await?;
    }
    Ok(())
}

async fn seed_speedtests(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot1_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_1.parse().unwrap(),
            serial: "serial1".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        let hotspot2_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_2.parse().unwrap(),
            serial: "serial2".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        let hotspot3_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_3.parse().unwrap(),
            serial: "serial3".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        let hotspot4_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_4.parse().unwrap(),
            serial: "serial4".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        speedtests::save_speedtest(&hotspot1_speedtest, txn).await?;
        speedtests::save_speedtest(&hotspot2_speedtest, txn).await?;
        speedtests::save_speedtest(&hotspot3_speedtest, txn).await?;
        speedtests::save_speedtest(&hotspot4_speedtest, txn).await?;
    }
    Ok(())
}

async fn seed_radio_thresholds(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let report1 = RadioThresholdIngestReport {
        received_timestamp: Default::default(),
        report: RadioThresholdReportReq {
            hotspot_pubkey: HOTSPOT_1.parse().unwrap(),
            cbsd_id: Some("".to_string()),
            bytes_threshold: 1000000,
            subscriber_threshold: 3,
            threshold_timestamp: ts,
            carrier_pub_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
        },
    };
    let report2 = RadioThresholdIngestReport {
        received_timestamp: Default::default(),
        report: RadioThresholdReportReq {
            hotspot_pubkey: HOTSPOT_2.parse().unwrap(),
            cbsd_id: None,
            bytes_threshold: 1000000,
            subscriber_threshold: 3,
            threshold_timestamp: ts,
            carrier_pub_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
        },
    };
    let report3 = RadioThresholdIngestReport {
        received_timestamp: Default::default(),
        report: RadioThresholdReportReq {
            hotspot_pubkey: HOTSPOT_3.parse().unwrap(),
            cbsd_id: Some("".to_string()),
            bytes_threshold: 1000000,
            subscriber_threshold: 3,
            threshold_timestamp: ts,
            carrier_pub_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
        },
    };
    let cbsd_id = Some("P27-SCE4255W0002".to_string());
    let report4 = RadioThresholdIngestReport {
        received_timestamp: Default::default(),
        report: RadioThresholdReportReq {
            hotspot_pubkey: HOTSPOT_4.parse().unwrap(),
            cbsd_id,
            bytes_threshold: 1000000,
            subscriber_threshold: 3,
            threshold_timestamp: ts,
            carrier_pub_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
        },
    };
    radio_threshold::save(&report1, txn).await?;
    radio_threshold::save(&report2, txn).await?;
    radio_threshold::save(&report3, txn).await?;
    radio_threshold::save(&report4, txn).await?;
    Ok(())
}

fn create_coverage_object(
    ts: DateTime<Utc>,
    cbsd_id: Option<String>,
    pub_key: PublicKeyBinary,
    hex: u64,
    indoor: bool,
) -> CoverageObject {
    let location = h3o::CellIndex::try_from(hex).unwrap();
    let key_type = match cbsd_id {
        Some(s) => KeyType::CbsdId(s),
        None => KeyType::HotspotKey(pub_key.clone()),
    };
    let report = FSCoverageObject {
        pub_key,
        uuid: Uuid::new_v4(),
        key_type,
        coverage_claim_time: ts,
        coverage: vec![RadioHexSignalLevel {
            location,
            signal_level: SignalLevel::High,
            signal_power: 1000,
        }],
        indoor,
        trust_score: 1000,
        signature: Vec::new(),
    };
    CoverageObject {
        coverage_object: report,
        validity: CoverageObjectValidity::Valid,
    }
}

fn create_multi_coverage_object(
    ts: DateTime<Utc>,
    cbsd_id: Option<String>,
    pub_key: PublicKeyBinary,
    hex: Vec<u64>,
    indoor: bool,
) -> CoverageObject {
    let key_type = match cbsd_id {
        Some(s) => KeyType::CbsdId(s),
        None => KeyType::HotspotKey(pub_key.clone()),
    };
    let coverage: Vec<RadioHexSignalLevel> = hex
        .iter()
        .map(|h| RadioHexSignalLevel {
            location: h3o::CellIndex::try_from(*h).unwrap(),
            signal_level: SignalLevel::High,
            signal_power: 1000,
        })
        .collect();

    let report = FSCoverageObject {
        pub_key,
        uuid: Uuid::new_v4(),
        key_type,
        coverage_claim_time: ts,
        coverage,
        indoor,
        trust_score: 1000,
        signature: Vec::new(),
    };
    CoverageObject {
        coverage_object: report,
        validity: CoverageObjectValidity::Valid,
    }
}
async fn save_seniority_object(
    ts: DateTime<Utc>,
    hb: &ValidatedHeartbeat,
    exec: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO seniority
          (radio_key, last_heartbeat, uuid, seniority_ts, inserted_at, update_reason, radio_type)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(hb.heartbeat.key())
    .bind(hb.heartbeat.timestamp)
    .bind(hb.heartbeat.coverage_object)
    .bind(ts)
    .bind(ts)
    .bind(SeniorityUpdateReason::NewCoverageClaimTime as i32)
    .bind(hb.heartbeat.hb_type)
    .execute(&mut *exec)
    .await?;
    Ok(())
}

fn get_poc_allocation_buckets(epoch_duration: Duration) -> (Decimal, Decimal) {
    // To not deal with percentages of percentages, let's start with the
    // total emissions and work from there.
    let total_emissions = reward_shares::get_total_scheduled_tokens(epoch_duration);
    let data_transfer = total_emissions * dec!(0.4);
    let regular_poc = total_emissions * dec!(0.1);
    let boosted_poc = total_emissions * dec!(0.1);

    // There is no data transfer in this test to be rewarded, so we know
    // the entirety of the unallocated amount will be put in the poc
    // pool.
    let regular_poc = regular_poc + data_transfer;

    (regular_poc, boosted_poc)
}

/// Hex Boostable Radio Checklist:
/// - Coverage Object: 1
/// - Seniority Update: 1
/// - Heartbeats: 12
/// - Speedtests: 2
/// - Radio Threshold: 1
struct HexBoostableRadio {
    pubkey: PublicKeyBinary,
    cbsd_id: Option<String>,
    hb_type: HbType,
    is_indoor: bool,
    location: Cell,
    heartbeat_count: i64,
}

impl HexBoostableRadio {
    fn cbrs_indoor(pubkey: PublicKeyBinary, location: Cell) -> Self {
        Self::new(pubkey, BoostedHexDeviceType::CbrsIndoor, location)
    }

    fn new(pubkey: PublicKeyBinary, boost_type: BoostedHexDeviceType, location: Cell) -> Self {
        let (hb_type, is_indoor, cbsd_id) = match boost_type {
            BoostedHexDeviceType::All => panic!("a radio cannot be all types at once"),
            BoostedHexDeviceType::CbrsIndoor => {
                (HbType::Cbrs, true, Some(format!("cbsd-indoor-{pubkey}")))
            }
            BoostedHexDeviceType::CbrsOutdoor => {
                (HbType::Cbrs, false, Some(format!("cbsd-outdoor-{pubkey}")))
            }
            BoostedHexDeviceType::WifiIndoor => (HbType::Wifi, true, None),
            BoostedHexDeviceType::WifiOutdoor => (HbType::Wifi, false, None),
        };
        Self {
            pubkey,
            cbsd_id,
            hb_type,
            is_indoor,
            location,
            heartbeat_count: 12,
        }
    }

    async fn seed(
        self,
        timestamp: DateTime<Utc>,
        txn: &mut Transaction<'_, Postgres>,
    ) -> anyhow::Result<()> {
        let cov_obj = create_coverage_object(
            timestamp,
            self.cbsd_id.clone(),
            self.pubkey.clone(),
            self.location.into_raw(),
            self.is_indoor,
        );
        for n in 0..=self.heartbeat_count {
            let time_ahead = timestamp + ChronoDuration::hours(n);
            let time_behind = timestamp - ChronoDuration::hours(24);

            let wifi_heartbeat = ValidatedHeartbeat {
                heartbeat: Heartbeat {
                    hb_type: self.hb_type,
                    hotspot_key: self.pubkey.clone(),
                    cbsd_id: self.cbsd_id.clone(),
                    operation_mode: true,
                    lat: 0.0,
                    lon: 0.0,
                    coverage_object: Some(cov_obj.coverage_object.uuid),
                    location_validation_timestamp: Some(time_behind),
                    timestamp: time_ahead,
                },
                cell_type: CellType::NovaGenericWifiIndoor,
                distance_to_asserted: Some(10),
                coverage_meta: None,
                location_trust_score_multiplier: dec!(1.0),
                validity: HeartbeatValidity::Valid,
            };

            let speedtest = CellSpeedtest {
                pubkey: self.pubkey.clone(),
                serial: format!("serial-{}", self.pubkey),
                timestamp: timestamp - ChronoDuration::hours(n * 4),
                upload_speed: 100_000_000,
                download_speed: 100_000_000,
                latency: 49,
            };

            save_seniority_object(time_ahead, &wifi_heartbeat, txn).await?;
            wifi_heartbeat.save(txn).await?;
            speedtests::save_speedtest(&speedtest, txn).await?;
        }
        cov_obj.save(txn).await?;

        let report = RadioThresholdIngestReport {
            received_timestamp: Default::default(),
            report: RadioThresholdReportReq {
                hotspot_pubkey: self.pubkey.clone(),
                cbsd_id: self.cbsd_id.clone(),
                bytes_threshold: 1_000_000,
                subscriber_threshold: 3,
                threshold_timestamp: timestamp,
                carrier_pub_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
            },
        };
        radio_threshold::save(&report, txn).await?;

        Ok(())
    }
}
