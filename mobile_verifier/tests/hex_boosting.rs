mod common;
use crate::common::MockFileSinkReceiver;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Duration, Utc};
use file_store::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    mobile_radio_threshold::{RadioThresholdIngestReport, RadioThresholdReportReq},
    speedtest::CellSpeedtest,
};
use futures_util::{stream, StreamExt as FuturesStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, HeartbeatValidity, RadioReward, SeniorityUpdateReason, SignalLevel,
    UnallocatedReward,
};
use hextree::Cell;
use mobile_config::{
    boosted_hex_info::{BoostedHexInfo, BoostedHexInfoStream},
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
};
use mobile_verifier::{
    cell_type::CellType,
    coverage::{set_oracle_boosting_assignments, CoverageObject, UnassignedHex},
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

#[derive(Debug, Clone)]
pub struct MockHexBoostingClient {
    pub boosted_hexes: Vec<BoostedHexInfo>,
}

impl MockHexBoostingClient {
    fn new(boosted_hexes: Vec<BoostedHexInfo>) -> Self {
        Self { boosted_hexes }
    }
}

#[async_trait]
impl HexBoostingInfoResolver for MockHexBoostingClient {
    type Error = ClientError;

    async fn stream_boosted_hexes_info(&mut self) -> Result<BoostedHexInfoStream, ClientError> {
        Ok(stream::iter(self.boosted_hexes.clone()).boxed())
    }

    async fn stream_modified_boosted_hexes_info(
        &mut self,
        _timestamp: DateTime<Utc>,
    ) -> Result<BoostedHexInfoStream, ClientError> {
        Ok(stream::iter(self.boosted_hexes.clone()).boxed())
    }
}

async fn update_assignments(pool: &PgPool) -> anyhow::Result<()> {
    let unassigned_hexes = UnassignedHex::fetch(pool);
    let _ = set_oracle_boosting_assignments(
        unassigned_hexes,
        &common::MockHexAssignments::default(),
        pool,
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
        let exp_reward_1 = 31_729_243_786_356;
        let exp_reward_2 = 15_864_621_893_178;
        let exp_reward_3 = 1_586_462_189_317;

        assert_eq!(exp_reward_1, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the boosted hexes in the radio rewards
        // assert the number of boosted hexes for each radio
        assert_eq!(1, poc_rewards[0].boosted_hexes.len());
        assert_eq!(1, poc_rewards[1].boosted_hexes.len());
        // hotspot 3 has no boosted hexes as all its hex boosts are 1x multiplier
        // and those get filtered out as they dont affect points
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // assert the hex boost multiplier values
        assert_eq!(20, poc_rewards[0].boosted_hexes[0].multiplier);
        assert_eq!(10, poc_rewards[1].boosted_hexes[0].multiplier);

        // assert the hex boost location values
        assert_eq!(
            0x8a1fb49642dffff_u64,
            poc_rewards[0].boosted_hexes[0].location
        );
        assert_eq!(
            0x8a1fb466d2dffff_u64,
            poc_rewards[1].boosted_hexes[0].location
        );

        // hotspot1 should have 20x the reward of hotspot 3
        assert_eq!(poc_rewards[0].poc_reward / poc_rewards[2].poc_reward, 20);
        // hotspot1 should have 10x the reward of hotspot 3
        assert_eq!(poc_rewards[1].poc_reward / poc_rewards[2].poc_reward, 10);

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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
        let exp_reward_1 = 16393442622950;
        let exp_reward_2 = 16393442622950;
        let exp_reward_3 = 16393442622950;

        assert_eq!(exp_reward_1, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        assert_eq!(0, poc_rewards[0].boosted_hexes.len());
        assert_eq!(0, poc_rewards[1].boosted_hexes.len());
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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
        let exp_reward_1 = 23_990_403_838_464;
        let exp_reward_2 = 23_990_403_838_464;
        let exp_reward_3 = 1_199_520_191_923;

        assert_eq!(exp_reward_1, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        assert_eq!(1, poc_rewards[0].boosted_hexes.len());
        assert_eq!(2, poc_rewards[1].boosted_hexes.len());
        // hotspot 3 has no boosted hexes as all its hex boosts are 1x multiplier
        // and those get filtered out as they dont affect points
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // assert the hex boost multiplier values
        // as hotspot 3 has 2 covered hexes, it should have 2 boosted hexes
        // sort order in the vec for these is not guaranteed, so sort them
        let mut hotspot_1_boosted_hexes = poc_rewards[1].boosted_hexes.clone();
        hotspot_1_boosted_hexes.sort_by(|a, b| b.location.cmp(&a.location));

        assert_eq!(20, poc_rewards[0].boosted_hexes[0].multiplier);
        assert_eq!(10, hotspot_1_boosted_hexes[1].multiplier);
        assert_eq!(10, hotspot_1_boosted_hexes[1].multiplier);

        // assert the hex boost location values
        assert_eq!(0x8a1fb46622dffff_u64, hotspot_1_boosted_hexes[0].location);
        assert_eq!(0x8a1fb46622d7fff_u64, hotspot_1_boosted_hexes[1].location);
        assert_eq!(
            0x8a1fb49642dffff_u64,
            poc_rewards[0].boosted_hexes[0].location
        );

        // hotspot1 should have 20x the reward of hotspot 3
        assert_eq!(poc_rewards[0].poc_reward / poc_rewards[2].poc_reward, 20);
        // hotspot1 should have 20x the reward of hotspot 3
        // due to the 2 boosted hexes each with a 10x multiplier
        assert_eq!(poc_rewards[1].poc_reward / poc_rewards[2].poc_reward, 20);

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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
    dbg!(epoch.start, start_ts_1, end_ts_1);
    // setup boosted hex where reward start time is same as the boost period ends
    let multipliers2 = vec![
        NonZeroU32::new(4).unwrap(),
        NonZeroU32::new(12).unwrap(),
        NonZeroU32::new(17).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * multipliers2.len() as i32);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);
    dbg!(epoch.start, start_ts_2, end_ts_2);

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
        let exp_reward_1 = 16_393_442_622_950;
        let exp_reward_2 = 16_393_442_622_950;
        let exp_reward_3 = 16_393_442_622_950;

        assert_eq!(exp_reward_1, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        // all will be zero as the boost period has expired for the single boosted hex
        assert_eq!(0, poc_rewards[0].boosted_hexes.len());
        assert_eq!(0, poc_rewards[1].boosted_hexes.len());
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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
    let boost_period_length = Duration::days(30);

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats_v3(epoch.start, &mut txn).await?;
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
        // HOTSPOT 1 has full location trust score and a boosted location
        // HOTSPOT 2 has full location trust score and NO boosted location
        // HOTSPOT 3 has reduced location trust score and a boosted location

        // assert poc reward outputs
        let hotspot_1_reward = 30_264_817_150_063;
        let hotspot_2_reward = 15_132_408_575_031;
        let hotspot_3_reward = 3_783_102_143_757;

        assert_eq!(hotspot_1_reward, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(hotspot_2_reward, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(hotspot_3_reward, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the boosted hexes in the radio rewards
        // assert the number of boosted hexes for each radio

        //hotspot 1 has one boosted hex
        assert_eq!(1, poc_rewards[1].boosted_hexes.len());
        //hotspot 2 has no boosted hexes
        assert_eq!(0, poc_rewards[0].boosted_hexes.len());
        // hotspot 3 has a boosted location but as its location trust score
        // is reduced the boost does not get applied
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // assert the hex boost multiplier values
        // assert_eq!(2, poc_rewards[0].boosted_hexes[0].multiplier);
        assert_eq!(2, poc_rewards[1].boosted_hexes[0].multiplier);

        assert_eq!(
            0x8a1fb466d2dffff_u64,
            poc_rewards[1].boosted_hexes[0].location
        );

        // hotspot1 should have 2x the reward of hotspot 2
        // hotspot 2 has a full location trust score but no boosted hex
        assert_eq!(poc_rewards[1].poc_reward / poc_rewards[0].poc_reward, 2);
        // hotspot1 should have 8x the reward of hotspot 3
        // hotspot 2 has a reduced location trust score and thus gets no boost
        // even tho its location is boosted
        // this results in a 4x reduction in reward compared to hotspot 1's base reward
        // then when you apply hotspots 2x boost you get an 8x difference
        assert_eq!(poc_rewards[1].poc_reward / poc_rewards[2].poc_reward, 8);

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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
async fn test_poc_with_cbrs_and_multi_coverage_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
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
        let exp_reward_1 = 23_990_403_838_464;
        let exp_reward_2 = 23_990_403_838_464;
        let exp_reward_3 = 1_199_520_191_923;

        assert_eq!(exp_reward_1, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_2, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(exp_reward_3, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_4.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the number of boosted hexes for each radio
        assert_eq!(1, poc_rewards[0].boosted_hexes.len());
        assert_eq!(2, poc_rewards[1].boosted_hexes.len());
        // hotspot 3 has no boosted hexes as all its hex boosts are 1x multiplier
        // and those get filtered out as they dont affect points
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // assert the hex boost multiplier values
        // as hotspot 3 has 2 covered hexes, it should have 2 boosted hexes
        // sort order in the vec for these is not guaranteed, so sort them
        let mut hotspot_1_boosted_hexes = poc_rewards[1].boosted_hexes.clone();
        hotspot_1_boosted_hexes.sort_by(|a, b| b.location.cmp(&a.location));

        assert_eq!(20, poc_rewards[0].boosted_hexes[0].multiplier);
        assert_eq!(10, hotspot_1_boosted_hexes[1].multiplier);
        assert_eq!(10, hotspot_1_boosted_hexes[1].multiplier);

        // assert the hex boost location values
        assert_eq!(0x8a1fb46622dffff_u64, hotspot_1_boosted_hexes[0].location);
        assert_eq!(0x8a1fb46622d7fff_u64, hotspot_1_boosted_hexes[1].location);
        assert_eq!(
            0x8a1fb49642dffff_u64,
            poc_rewards[0].boosted_hexes[0].location
        );

        // hotspot1 should have 20x the reward of hotspot 3
        assert_eq!(poc_rewards[0].poc_reward / poc_rewards[2].poc_reward, 20);
        // hotspot1 should have 20x the reward of hotspot 3
        // due to the 2 boosted hexes each with a 10x multiplier
        assert_eq!(poc_rewards[1].poc_reward / poc_rewards[2].poc_reward, 20);

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
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

async fn receive_expected_rewards(
    mobile_rewards: &mut MockFileSinkReceiver,
) -> anyhow::Result<(Vec<RadioReward>, UnallocatedReward)> {
    // get the filestore outputs from rewards run
    // we will have 3 radio rewards, 1 wifi radio and 2 cbrs radios
    let radio_reward1 = mobile_rewards.receive_radio_reward().await;
    let radio_reward2 = mobile_rewards.receive_radio_reward().await;
    let radio_reward3 = mobile_rewards.receive_radio_reward().await;
    // ordering is not guaranteed, so stick the rewards into a vec and sort
    let mut poc_rewards = vec![radio_reward1, radio_reward2, radio_reward3];
    // after sorting reward 1 = cbrs radio1, 2 = cbrs radio2, 3 = wifi radio
    poc_rewards.sort_by(|a, b| b.hotspot_key.cmp(&a.hotspot_key));

    // expect one unallocated reward for poc
    let unallocated_poc_reward = mobile_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    mobile_rewards.assert_no_messages();

    Ok((poc_rewards, unallocated_poc_reward))
}

async fn seed_heartbeats_v1(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
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

async fn seed_heartbeats_v2(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();
        println!("0x8a1fb466d2dffff_u64 as u64: {}", 0x8a1fb466d2dffff_u64);
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

async fn seed_heartbeats_v3(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // HOTSPOT 1 has full location trust score
    // HOTSPOT 2 has full location trust score
    // HOTSPOT 3 has reduced location trust score
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
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(300),
            coverage_meta: None,
            location_trust_score_multiplier: dec!(0.25),
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
        println!("0x8a1fb466d2dffff_u64 as u64: {}", 0x8a1fb466d2dffff_u64);
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
