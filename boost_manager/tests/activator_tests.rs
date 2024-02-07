mod common;
use boost_manager::{activator, db, OnChainStatus};
use chrono::{DateTime, Duration as ChronoDuration, Duration, Timelike, Utc};
use helium_proto::services::poc_mobile::BoostedHex as BoostedHexProto;
use mobile_config::boosted_hex_info::{BoostedHexInfo, BoostedHexes};
use sqlx::PgPool;
use std::collections::HashMap;

const BOOST_CONFIG_PUBKEY: &str = "11hd7HoicRgBPjBGcqcT2Y9hRQovdZeff5eKFMbCSuDYQmuCiF1";

struct TestContext {
    boosted_hexes: Vec<BoostedHexInfo>,
}

impl TestContext {
    fn setup(now: DateTime<Utc>) -> anyhow::Result<Self> {
        let epoch = (now - ChronoDuration::hours(24))..now;
        let boost_period_length = Duration::days(30);

        // setup boosted hex data to stream as updates
        let multipliers1 = vec![2, 10, 15, 35];
        let start_ts_1 = epoch.start - boost_period_length;
        let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);
        let multipliers2 = vec![3, 10, 20];
        let start_ts_2 = epoch.start - (boost_period_length * 2);
        let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);
        let multipliers3 = vec![1, 10, 20];

        let boosts = vec![
            BoostedHexInfo {
                location: 0x8a1fb466d2dffff_u64,
                start_ts: Some(start_ts_1),
                end_ts: Some(end_ts_1),
                period_length: boost_period_length,
                multipliers: multipliers1,
                boosted_hex_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
                boost_config_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
            },
            BoostedHexInfo {
                location: 0x8a1fb49642dffff_u64,
                start_ts: Some(start_ts_2),
                end_ts: Some(end_ts_2),
                period_length: boost_period_length,
                multipliers: multipliers2,
                boosted_hex_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
                boost_config_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
            },
            BoostedHexInfo {
                // hotspot 3's location
                location: 0x8c2681a306607ff_u64,
                start_ts: None,
                end_ts: None,
                period_length: boost_period_length,
                multipliers: multipliers3,
                boosted_hex_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
                boost_config_pubkey: BOOST_CONFIG_PUBKEY.to_string(),
            },
        ];
        Ok(Self {
            boosted_hexes: boosts,
        })
    }
}
#[sqlx::test]
async fn test_activated_hex_insert(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let ctx = TestContext::setup(now)?;
    let boosted_hexes_map = ctx
        .boosted_hexes
        .iter()
        .map(|info| (info.location, info.clone()))
        .collect::<HashMap<_, _>>();
    let boosted_hexes = BoostedHexes {
        hexes: boosted_hexes_map,
    };

    // test a boosted hex derived from radio rewards
    // with a non set start date, will result in a row being
    // inserted to the activation table
    let mut txn = pool.clone().begin().await?;
    activator::process_boosted_hex(
        &mut txn,
        now,
        &boosted_hexes,
        &BoostedHexProto {
            location: 0x8c2681a306607ff_u64,
            multiplier: 10,
        },
    )
    .await?;
    txn.commit().await?;
    let rows = db::get_queued_batch(&pool).await?;
    assert_eq!(rows.len(), 1);
    let status = db::query_activation_statuses(&pool).await?;
    assert_eq!(status[0].status, OnChainStatus::Queued);
    assert_eq!(status[0].location, 0x8c2681a306607ff_u64);

    Ok(())
}

#[sqlx::test]
async fn test_activated_hex_no_insert(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let ctx = TestContext::setup(now)?;
    let boosted_hexes_map = ctx
        .boosted_hexes
        .iter()
        .map(|info| (info.location, info.clone()))
        .collect::<HashMap<_, _>>();
    let boosted_hexes = BoostedHexes {
        hexes: boosted_hexes_map,
    };

    // test a boosted hex derived from radio rewards
    // with an active start date, will result in no row being
    // inserted to the activation table
    let mut txn = pool.clone().begin().await?;
    activator::process_boosted_hex(
        &mut txn,
        now,
        &boosted_hexes,
        &BoostedHexProto {
            location: 0x8a1fb49642dffff_u64,
            multiplier: 10,
        },
    )
    .await?;
    txn.commit().await?;
    let rows = db::get_queued_batch(&pool).await?;
    assert_eq!(rows.len(), 0);
    Ok(())
}

#[sqlx::test]
async fn test_activated_dup_hex_insert(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now().with_second(0).unwrap();
    let ctx = TestContext::setup(now)?;
    let boosted_hexes_map = ctx
        .boosted_hexes
        .iter()
        .map(|info| (info.location, info.clone()))
        .collect::<HashMap<_, _>>();
    let boosted_hexes = BoostedHexes {
        hexes: boosted_hexes_map,
    };

    // test with DUPLICATE boosted hexes derived from radio rewards
    // with a non set start date, will result in a single row being
    // inserted to the activation table with an activation ts
    // equal to the first hex processed
    let mut txn = pool.clone().begin().await?;
    activator::process_boosted_hex(
        &mut txn,
        now,
        &boosted_hexes,
        &BoostedHexProto {
            location: 0x8c2681a306607ff_u64,
            multiplier: 10,
        },
    )
    .await?;

    activator::process_boosted_hex(
        &mut txn,
        now - ChronoDuration::days(1),
        &boosted_hexes,
        &BoostedHexProto {
            location: 0x8c2681a306607ff_u64,
            multiplier: 5,
        },
    )
    .await?;

    txn.commit().await?;
    let rows1 = db::get_queued_batch(&pool).await?;
    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0].activation_ts, now);
    Ok(())
}
