use crate::common::{
    self, default_price_info, reward_info_24_hours, AsStringKeyedMap, MockHexBoostingClient,
    RadioRewardV2Ext,
};
use chrono::{DateTime, Duration as ChronoDuration, Duration, Utc};
use file_store_oracles::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    speedtest::CellSpeedtest,
    unique_connections::{UniqueConnectionReq, UniqueConnectionsIngestReport},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, HeartbeatValidity, LocationSource, SeniorityUpdateReason, SignalLevel,
};
use hextree::Cell;
use mobile_config::{boosted_hex_info::BoostedHexInfo, sub_dao_epoch_reward_info::EpochRewardInfo};
use mobile_verifier::{
    cell_type::CellType,
    coverage::CoverageObject,
    heartbeats::{HbType, Heartbeat, ValidatedHeartbeat},
    reward_shares, rewarder, speedtests,
    unique_connections::{self, MINIMUM_UNIQUE_CONNECTIONS},
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use solana::SolPubkey;
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
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have a coverage reports for a singular hex location per radio
    seed_heartbeats_v1(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = reward_info.epoch_period.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = reward_info.epoch_period.start - (boost_period_length * 2);
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
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let price_info = default_price_info();

    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1");
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2");
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3");

    assert_eq!(poc_rewards.len(), 3);

    // Calculating expected rewards
    let regular_poc = get_poc_allocation_buckets(reward_info.epoch_emissions);

    // With regular poc now 50% of total emissions, that will be split
    // between the 3 radios equally. 900 comes from IndoorWifi 400 *
    // 0.75 speedtest multiplier * 3 radios

    // Boosted hexes are 10x and 20x.
    // (300 * 19) + (300 * 9) = 8400;
    // To get points _only_ from boosting.

    //combined is 9300 points
    let share = regular_poc / dec!(9300);

    let exp_reward_1 = rounded(share * dec!(300)) + rounded(share * dec!(300) * dec!(19));
    let exp_reward_2 = rounded(share * dec!(300)) + rounded(share * dec!(300) * dec!(9));
    let exp_reward_3 = rounded(share * dec!(300)) + rounded(share * dec!(300) * dec!(0));

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

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_poc_boosted_hexes_unique_connections_not_seeded(pool: PgPool) -> anyhow::Result<()> {
    // this is the same setup as the previous one, but with unique connections data not seeded
    // this simulates the case where we have US radios in boosted hexes but where the unique
    // connections requirement has not been met (HIP-140)
    // the end result is that no boosting takes place, the radios are awarded non boosted reward values
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have a coverage reports for a singular hex location per radio
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
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let reward_info = reward_info_24_hours();
    let price_info = default_price_info();

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1");
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2");
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3");

    assert_eq!(19_178_082_191_780, hotspot_1.total_poc_reward());
    assert_eq!(19_178_082_191_780, hotspot_2.total_poc_reward());
    assert_eq!(19_178_082_191_780, hotspot_3.total_poc_reward());

    // assert the number of boosted hexes for each radio
    assert_eq!(0, hotspot_1.boosted_hexes_len());
    assert_eq!(0, hotspot_2.boosted_hexes_len());
    assert_eq!(0, hotspot_3.boosted_hexes_len());

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_poc_with_multi_coverage_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    // seed HBs where we have multiple coverage reports for one radio and one report for the others
    seed_heartbeats_v2(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = reward_info.epoch_period.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let start_ts_2 = reward_info.epoch_period.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where reward start time is in the first period length
    // default to 1x multiplier for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];

    let start_ts_3 = reward_info.epoch_period.start;
    let end_ts_3 = start_ts_3 + (boost_period_length * multipliers3.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's first covered location
            location: Cell::from_raw(0x8a1fb46622dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1.clone(),
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 1's second covered location
            location: Cell::from_raw(0x8a1fb46622d7fff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: Some(start_ts_3),
            end_ts: Some(end_ts_3),
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &MockHexBoostingClient::new(boosted_hexes),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1");
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2");
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3");

    // Calculating expected rewards
    // - 2 covered hexes boosted at 10x
    // - 1 covered hex boosted at 20x
    // - 1 covered hex no boost
    let regular_poc = get_poc_allocation_buckets(reward_info.epoch_emissions);

    // With regular poc now 50% of total emissions, that will be split
    // between the 3 radios equally.
    // 1200 comes from IndoorWifi 400 * 0.75 speedtest multiplier * 4 hexes
    // let regular_share = regular_poc / dec!(1200);

    // Boosted hexes are 2 at 10x and 1 at 20x.
    // (300 * (9 * 2)) + (300 * 19) = 11,100;
    // To get points _only_ from boosting.
    // let boosted_share = boosted_poc / dec!(11_100);

    // combined points is 12,300
    let share = regular_poc / dec!(12_300);

    let hex_coverage = |hexes: u8| share * dec!(300) * Decimal::from(hexes);
    let boost_coverage = |mult: u8| share * dec!(300) * Decimal::from(mult);

    let exp_reward_1 = rounded(hex_coverage(2)) + rounded(boost_coverage(18));
    let exp_reward_2 = rounded(hex_coverage(1)) + rounded(boost_coverage(19));
    let exp_reward_3 = rounded(hex_coverage(1)) + rounded(boost_coverage(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward()); // 2 at 10x boost
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward()); // 1 at 20x boost
    assert_eq!(exp_reward_3, hotspot_3.total_poc_reward()); // 1 at no boost

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

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_expired_boosted_hex(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_v1(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is after the boost period ends
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
    ];
    let start_ts_1 = reward_info.epoch_period.start
        - (boost_period_length * multipliers1.len() as i32 + ChronoDuration::days(1));
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is same as the boost period ends
    let multipliers2 = vec![
        NonZeroU32::new(4).unwrap(),
        NonZeroU32::new(12).unwrap(),
        NonZeroU32::new(17).unwrap(),
    ];
    let start_ts_2 =
        reward_info.epoch_period.start - (boost_period_length * multipliers2.len() as i32);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb466d2dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &MockHexBoostingClient::new(boosted_hexes),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1");
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2");
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3");

    assert_eq!(poc_rewards.len(), 3);

    // assert poc reward outputs
    assert_eq!(19_178_082_191_780, hotspot_1.total_poc_reward());
    assert_eq!(19_178_082_191_780, hotspot_2.total_poc_reward());
    assert_eq!(19_178_082_191_780, hotspot_3.total_poc_reward());

    // assert the number of boosted hexes for each radio
    // all will be zero as the boost period has expired for the single boosted hex
    assert_eq!(0, hotspot_1.boosted_hexes_len());
    assert_eq!(0, hotspot_2.boosted_hexes_len());
    assert_eq!(0, hotspot_3.boosted_hexes_len());

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_reduced_location_score_with_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_with_location_trust(
        reward_info.epoch_period.start,
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
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![NonZeroU32::new(2).unwrap()];
    let start_ts_1 = reward_info.epoch_period.start;
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
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &MockHexBoostingClient::new(boosted_hexes),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1"); // full location trust 1 boost
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2"); // full location NO boosts
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3"); // reduced location trust 1 boost

    assert_eq!(poc_rewards.len(), 3);

    // Calculating expected rewards
    let regular_poc = get_poc_allocation_buckets(reward_info.epoch_emissions);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 0.25     | 75    |
    // |---------------------|-----------|----------|-------|
    //                                              | 675   |

    // Boosted hexes are 2x, only one radio qualifies based on the location trust
    // 300 * 1 == 300
    // To get points _only_ from boosting.

    // combined points is 975
    let share = regular_poc / dec!(975);

    let exp_reward_1 = rounded(share * dec!(300)) + rounded(share * dec!(300) * dec!(1));
    let exp_reward_2 = rounded(share * dec!(300)) + rounded(share * dec!(300) * dec!(0));
    let exp_reward_3 = rounded(share * dec!(75)) + rounded(share * dec!(75) * dec!(0));

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

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_distance_from_asserted_removes_boosting_but_not_location_trust(
    pool: PgPool,
) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_with_location_trust(
        reward_info.epoch_period.start,
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
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![NonZeroU32::new(2).unwrap()];
    let start_ts_1 = reward_info.epoch_period.start;
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
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: None,
            end_ts: None,
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &MockHexBoostingClient::new(boosted_hexes),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1"); // full location trust 1 boost
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2"); // full location trust NO boosts
    let hotspot_3 = poc_rewards.get(HOTSPOT_3).expect("hotspot 3"); // reduced location trust 1 boost

    assert_eq!(poc_rewards.len(), 3);

    // Calculating expected rewards
    let regular_poc = get_poc_allocation_buckets(reward_info.epoch_emissions);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // |---------------------|-----------|----------|-------|
    //                                              | 900   |

    // Boosted hexes are 2x, only one radio qualifies based on the location trust
    // 300 * 1 == 300
    // To get points _only_ from boosting.

    // combined base poc points and boosted poc points is 1200
    let regular_share = regular_poc / dec!(1200);

    let exp_reward_1 =
        rounded(regular_share * dec!(300)) + rounded(regular_share * dec!(300) * dec!(1));
    let exp_reward_2 =
        rounded(regular_share * dec!(300)) + rounded(regular_share * dec!(300) * dec!(0));
    let exp_reward_3 =
        rounded(regular_share * dec!(300)) + rounded(regular_share * dec!(300) * dec!(0));

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

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

#[sqlx::test]
async fn test_poc_with_wifi_and_multi_coverage_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_v4(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_unique_connections(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // setup boosted hex where reward start time is in the second period length
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = reward_info.epoch_period.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);

    // setup boosted hex where reward start time is in the third & last period length
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = reward_info.epoch_period.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    // setup boosted hex where reward start time is in the first period length
    // default to 1x multiplier for easy math when comparing relative rewards
    let multipliers3 = vec![
        NonZeroU32::new(1).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_3 = reward_info.epoch_period.start;
    let end_ts_3 = start_ts_3 + (boost_period_length * multipliers3.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            // hotspot 1's first covered location
            location: Cell::from_raw(0x8a1fb46622dffff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1.clone(),
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 1's second covered location
            location: Cell::from_raw(0x8a1fb46622d7fff_u64)?,
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 2's location
            location: Cell::from_raw(0x8a1fb49642dffff_u64)?,
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            // hotspot 3's location
            location: Cell::from_raw(0x8c2681a306607ff_u64)?,
            start_ts: Some(start_ts_3),
            end_ts: Some(end_ts_3),
            period_length: boost_period_length,
            multipliers: multipliers3,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &MockHexBoostingClient::new(boosted_hexes),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let poc_rewards = rewards.radio_reward_v2s.as_keyed_map();
    let hotspot_1 = poc_rewards.get(HOTSPOT_1).expect("hotspot 1"); // 2 boosts at 10x
    let hotspot_2 = poc_rewards.get(HOTSPOT_2).expect("hotspot 2"); // 1 boost at 20x
    let hotspot_4 = poc_rewards.get(HOTSPOT_4).expect("hotspot 4"); // no boosts

    assert_eq!(poc_rewards.len(), 3);

    // Calculating expected rewards
    let regular_poc = get_poc_allocation_buckets(reward_info.epoch_emissions);

    // Here's how we get the regular shares per coverage points
    // | base coverage point | speedtest | location | total |
    // |---------------------|-----------|----------|-------|
    // | 120 x 2             | 0.75      | 1.00     | 180   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // | 400                 | 0.75      | 1.00     | 300   |
    // |---------------------|-----------|----------|-------|
    //                                              | 780   |

    // Boosted hexes are 1 at 10x and 1 at 20x.
    // Only wifi is targeted with Boosts.
    // (180 * 9) + (300 * 19) == 7320
    // To get points _only_ from boosting.
    //
    // combined points is 8100
    let share = regular_poc / dec!(8100);

    let exp_reward_1 =
        rounded(share * (dec!(180) * dec!(1))) + rounded(share * (dec!(180) * dec!(9)));
    let exp_reward_2 = rounded(share * dec!(300) * dec!(1)) + rounded(share * dec!(300) * dec!(19));
    let exp_reward_3 = rounded(share * dec!(300) * dec!(1)) + rounded(share * dec!(300) * dec!(0));

    assert_eq!(exp_reward_1, hotspot_1.total_poc_reward());
    assert_eq!(exp_reward_2, hotspot_2.total_poc_reward());
    assert_eq!(exp_reward_3, hotspot_4.total_poc_reward());

    // assert the number of boosted hexes for each radio
    assert_eq!(1, hotspot_2.boosted_hexes_len());
    assert_eq!(2, hotspot_1.boosted_hexes_len());
    assert_eq!(1, hotspot_4.boosted_hexes_len());

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

    // confirm the total rewards allocated matches emissions
    // and the rewarded percentage amount matches percentage
    let total = rewards.total_poc_rewards() + rewards.unallocated_amount_or_default();
    assert_total_matches_emissions(total, &reward_info);

    Ok(())
}

fn rounded(num: Decimal) -> u64 {
    num.to_u64().unwrap_or_default()
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
            hotspot_key1.clone(),
            vec![0x8a1fb46622dffff_u64, 0x8a1fb46622d7fff_u64],
            true,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
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
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
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
            hotspot_key3.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key3,
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
            hotspot_key1.clone(),
            0x8a1fb466d2dffff_u64,
            true,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
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
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
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
            hotspot_key3.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key3,

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
            hotspot_key1.clone(),
            [0x8a1fb46622dffff_u64, 0x8a1fb46622d7fff_u64].into(),
            false,
        );
        let wifi_heartbeat1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,

                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiOutdoor,
            distance_to_asserted: Some(10),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,

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
        let cov_obj_3 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            hotspot_key4.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key4,

                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(1),
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

async fn seed_unique_connections(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let reports: Vec<_> = vec![HOTSPOT_1, HOTSPOT_2, HOTSPOT_3, HOTSPOT_4]
        .into_iter()
        .map(|key| UniqueConnectionsIngestReport {
            received_timestamp: ts,
            report: UniqueConnectionReq {
                pubkey: key.parse().unwrap(),
                start_timestamp: ts,
                end_timestamp: ts + Duration::hours(24),
                unique_connections: MINIMUM_UNIQUE_CONNECTIONS + 1,
                timestamp: ts,
                carrier_key: CARRIER_HOTSPOT_KEY.parse().unwrap(),
                signature: vec![],
            },
        })
        .collect();

    unique_connections::db::save(txn, &reports).await?;

    Ok(())
}

fn create_coverage_object(
    ts: DateTime<Utc>,
    pub_key: PublicKeyBinary,
    hex: u64,
    indoor: bool,
) -> CoverageObject {
    let location = h3o::CellIndex::try_from(hex).unwrap();
    let key_type = KeyType::HotspotKey(pub_key.clone());
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
    pub_key: PublicKeyBinary,
    hex: Vec<u64>,
    indoor: bool,
) -> CoverageObject {
    let key_type = KeyType::HotspotKey(pub_key.clone());

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
    .execute(&mut **exec)
    .await?;
    Ok(())
}

fn get_poc_allocation_buckets(total_emissions: Decimal) -> Decimal {
    // To not deal with percentages of percentages, let's start with the
    // total emissions and work from there.
    let data_transfer = total_emissions * dec!(0.7);
    let regular_poc = total_emissions * dec!(0.0);

    // There is no data transfer in this test to be rewarded, so we know
    // the entirety of the unallocated amount will be put in the poc
    // pool.
    regular_poc + data_transfer
}

fn assert_total_matches_emissions(total: u64, reward_info: &EpochRewardInfo) {
    // confirm the total rewards allocated matches expectations
    let total_poc_emissions =
        reward_shares::get_scheduled_tokens_for_poc(reward_info.epoch_emissions)
            .to_u64()
            .unwrap();
    assert_eq!(
        total_poc_emissions, total,
        "total does not match expected emissions"
    );

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(total) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.70), "POC and DC is always 70%");
}
