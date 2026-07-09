//! Integration tests for the HIP-149 reward path (`reward_dc_hip_149`): data
//! transfer consumes the entire emissions pool and Proof-of-Coverage is not
//! rewarded. Each hotspot with burned data-transfer sessions splits the pool in
//! proportion to its data credits, with rounding dust emitted as a single
//! `UnallocatedReward`.

use crate::common::{self, default_price_info, reward_info_24_hours};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::data_transfer::burned_session::{
    self, IcebergBurnedDataTransferSession,
};
use mobile_config::sub_dao_epoch_reward_info::EpochRewardInfo;
use mobile_verifier::{
    data_session::{self, DataSessionSource},
    reward_shares, rewarder,
};
use rust_decimal::prelude::*;
use sqlx::{PgPool, Postgres, Transaction};

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const HOTSPOT_3: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const PAYER_1: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

#[sqlx::test]
async fn test_dc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    // seed data sessions land in both Postgres and Trino
    let mut txn = pool.begin().await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn, &harness).await?;
    txn.commit().await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);

    // run rewards for poc and dc (Compare: rewards from Postgres, validates Trino)
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    // PoC is disabled, so no radios are rewarded for coverage.
    let poc_rewards = &rewards.radio_reward_v2s;
    assert!(poc_rewards.is_empty());

    // DC now consumes the whole data-transfer pool. The three hotspots burned
    // equal DC, so they split the pool evenly (scaled up well past their raw DC
    // value).
    let dc_rewards = &rewards.gateway_rewards;
    assert_eq!(dc_rewards.len(), 3);
    assert_eq!(
        dc_rewards[0].dc_transfer_reward,
        dc_rewards[1].dc_transfer_reward
    );
    assert_eq!(
        dc_rewards[1].dc_transfer_reward,
        dc_rewards[2].dc_transfer_reward
    );
    assert!(dc_rewards[0].dc_transfer_reward > 0, "allocation not zero");

    // The pool is fully accounted for: DC rewards plus rounding dust (written as
    // unallocated) equal the data-transfer allocation.
    let dc_sum = rewards.dc_transfer_sum();
    let unallocated_sum = rewards.unallocated_sum();
    let expected_sum = expected_data_transfer_pool(&reward_info);
    assert_eq!(dc_sum + unallocated_sum, expected_sum);

    // Only rounding dust is left over, not a real share.
    for reward in dc_rewards {
        assert!(
            unallocated_sum < reward.dc_transfer_reward,
            "unallocated should never exceed an individual gateways rewards"
        );
    }

    Ok(())
}

#[sqlx::test]
async fn test_no_data_sessions_unallocate_whole_pool(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    // The iceberg tables are created but no data sessions are seeded into either
    // backend, so there is nothing to distribute against.
    let harness = common::setup_iceberg().await?;
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);

    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    // No DC rewards; the entire data-transfer pool falls through to a single
    // unallocated reward.
    assert!(rewards.gateway_rewards.is_empty());
    assert_eq!(rewards.unallocated.len(), 1);

    let expected_sum = expected_data_transfer_pool(&reward_info);
    assert_eq!(rewards.unallocated_sum(), expected_sum);

    Ok(())
}

#[sqlx::test]
async fn test_unequal_dc_rewards_proportionally(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    // 1x / 2x / 3x DC, well under the pool — rewards scale up but keep the ratio.
    let sessions = data_sessions_with_dc(
        reward_info.epoch_period.start,
        &[
            (HOTSPOT_1, 1_000_000),
            (HOTSPOT_2, 2_000_000),
            (HOTSPOT_3, 3_000_000),
        ],
    );
    let mut txn = pool.begin().await?;
    write_sessions(&mut txn, &harness, &sessions).await?;
    txn.commit().await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    // DC is strictly increasing, so the sorted rewards are HOTSPOT_1/2/3's and
    // should track the 1:2:3 ratio (modulo per-hotspot floor rounding).
    let mut amounts: Vec<i64> = rewards
        .gateway_rewards
        .iter()
        .map(|r| r.dc_transfer_reward as i64)
        .collect();

    amounts.sort_unstable();
    assert_eq!(amounts.len(), 3);

    let (r1, r2, r3) = (amounts[0], amounts[1], amounts[2]);
    assert!(r1 > 0);
    assert!((r2 - (2 * r1)).abs() <= 2, "expected ~2x: {r1} vs {r2}");
    assert!((r3 - (3 * r1)).abs() <= 3, "expected ~3x: {r1} vs {r3}");

    // The whole pool is still distributed.
    let expected_sum = expected_data_transfer_pool(&reward_info);
    assert_eq!(
        rewards.dc_transfer_sum() + rewards.unallocated_sum(),
        expected_sum
    );

    Ok(())
}

#[sqlx::test]
async fn test_oversubscribed_distributes_whole_pool(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    // Burn far more DC than the pool covers at face value. Unlike the old
    // allocator (which capped each hotspot at its DC's HNT value and spilled the
    // remainder into PoC), HIP-149 still distributes the entire pool, scaled down.
    let dc_per_hotspot = 1_000_000_000_000_000; // 1e15
    let sessions = data_sessions_with_dc(
        reward_info.epoch_period.start,
        &[
            (HOTSPOT_1, dc_per_hotspot),
            (HOTSPOT_2, dc_per_hotspot),
            (HOTSPOT_3, dc_per_hotspot),
        ],
    );
    let mut txn = pool.begin().await?;
    write_sessions(&mut txn, &harness, &sessions).await?;
    txn.commit().await?;

    let price_info = default_price_info();
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        price_info.clone(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    let dc_rewards = &rewards.gateway_rewards;
    assert_eq!(dc_rewards.len(), 3);

    // Demand exceeds the pool, so each hotspot is scaled *below* its DC's HNT value.
    let dc_value =
        reward_shares::dc_to_hnt_bones(Decimal::from(dc_per_hotspot), price_info.price_per_bone)
            .to_u64()
            .unwrap();
    for reward in dc_rewards {
        assert!(
            reward.dc_transfer_reward < dc_value,
            "oversubscribed rewards should scale below raw DC value"
        );
    }

    // ...but the whole pool is still distributed.
    let expected_sum = expected_data_transfer_pool(&reward_info);
    assert_eq!(
        rewards.dc_transfer_sum() + rewards.unallocated_sum(),
        expected_sum
    );

    Ok(())
}

#[sqlx::test]
async fn test_single_hotspot_takes_whole_pool(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    let sessions = data_sessions_with_dc(reward_info.epoch_period.start, &[(HOTSPOT_1, 5_000_000)]);
    let mut txn = pool.begin().await?;
    write_sessions(&mut txn, &harness, &sessions).await?;
    txn.commit().await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    // The lone hotspot's share is 100% of the pool, so it consumes it whole with
    // no rounding remainder.
    let expected_sum = expected_data_transfer_pool(&reward_info);
    assert_eq!(rewards.gateway_rewards.len(), 1);
    assert_eq!(rewards.gateway_rewards[0].dc_transfer_reward, expected_sum);
    assert_eq!(rewards.unallocated_sum(), 0);

    Ok(())
}

// HIP-149 sizes data transfer as the residual of `hnt_rewards_issued` after a flat
// 24% service-provider cut, so the 3× cap (which moves HNT into delegation) and the
// backstop (which re-emits it) land entirely on the data-transfer pool. The two
// tests below drive a capped and a backstopped epoch end-to-end and assert the
// distributed pool shrinks / grows accordingly — the only place that wiring is
// exercised at the integration level (the split math itself is unit-tested in
// `reward_shares::emissions_split`). Expectations are concrete, computed
// independently of `hip_149_reward_pools` so they can't pass by mirroring a bug in
// the code under test.

/// A round 1e12-bone pool. The 24% SP cut (240e9) clears the 45e9 subscriber floor,
/// and the residual data-transfer pools land on clean numbers.
const SPLIT_TEST_EMISSIONS: u64 = 1_000_000_000_000;
/// Baseline (6% delegation) data-transfer pool at [`SPLIT_TEST_EMISSIONS`]:
/// 94% issued (940e9) − 24% SP (240e9). The cap shrinks below this; the backstop
/// grows above it.
const BASELINE_DATA_POOL: u64 = 700_000_000_000;

/// [`reward_info_24_hours`] with the on-chain split overridden. `hnt_issued` is what
/// the chain handed this rewarder; `delegation` is paid to veHNT holders on-chain.
/// Emissions are their sum, so the service-provider pool (a flat 24% of emissions)
/// stays fixed while only the issued/delegation split moves.
fn reward_info_with_split(hnt_issued: u64, delegation: u64) -> EpochRewardInfo {
    let mut reward_info = reward_info_24_hours();
    reward_info.epoch_emissions = Decimal::from(hnt_issued + delegation);
    reward_info.hnt_rewards_issued = Decimal::from(hnt_issued);
    reward_info.delegation_rewards_issued = Decimal::from(delegation);
    reward_info
}

/// Data-transfer pool computed *independently* of `hip_149_reward_pools` (the code
/// the production path uses), via plain integer math: SP takes a flat floored 24%
/// of total emissions, data transfer is the rest of the issued HNT.
fn expected_data_transfer_pool(reward_info: &EpochRewardInfo) -> u64 {
    let emissions = reward_info.epoch_emissions.to_u64().unwrap();
    let hnt_issued = reward_info.hnt_rewards_issued.to_u64().unwrap();
    let service_provider = (emissions as u128 * 24 / 100) as u64;
    hnt_issued - service_provider
}

#[sqlx::test]
async fn test_cap_shrinks_data_transfer_pool(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    // 3× cap moved 14% of emissions out of the data bucket into delegation:
    // delegation 6%+14%=20%, issued HNT 80%. SP holds at 24%, so data transfer
    // absorbs the whole cut (70% → 56%).
    let reward_info = reward_info_with_split(
        SPLIT_TEST_EMISSIONS * 80 / 100,
        SPLIT_TEST_EMISSIONS * 20 / 100,
    );

    let harness = common::setup_iceberg().await?;
    let mut txn = pool.begin().await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn, &harness).await?;
    txn.commit().await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    assert_eq!(rewards.gateway_rewards.len(), 3);

    // The whole pool is distributed, and it is the cap-shrunk residual.
    let realized = rewards.dc_transfer_sum() + rewards.unallocated_sum();
    assert_eq!(realized, expected_data_transfer_pool(&reward_info));
    assert_eq!(realized, 560_000_000_000, "80% issued − 24% SP");
    assert!(
        realized < BASELINE_DATA_POOL,
        "cap must shrink the data-transfer pool below baseline"
    );

    Ok(())
}

#[sqlx::test]
async fn test_backstop_grows_data_transfer_pool(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    // Backstop re-emitted HNT into the data bucket: issued HNT rises to 98%
    // (delegation 2%). SP still holds at 24%, so data transfer absorbs the boost
    // (70% → 74%).
    let reward_info = reward_info_with_split(
        SPLIT_TEST_EMISSIONS * 98 / 100,
        SPLIT_TEST_EMISSIONS * 2 / 100,
    );

    let harness = common::setup_iceberg().await?;
    let mut txn = pool.begin().await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn, &harness).await?;
    txn.commit().await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc_hip_149(
        &DataSessionSource::new(pool.clone(), Some(trino)),
        mobile_rewards_client,
        &reward_info,
        default_price_info(),
        None,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    assert_eq!(rewards.gateway_rewards.len(), 3);

    // The whole pool is distributed, and it is the backstop-grown residual.
    let realized = rewards.dc_transfer_sum() + rewards.unallocated_sum();
    assert_eq!(realized, expected_data_transfer_pool(&reward_info));
    assert_eq!(realized, 740_000_000_000, "98% issued − 24% SP");
    assert!(
        realized > BASELINE_DATA_POOL,
        "backstop must grow the data-transfer pool above baseline"
    );

    Ok(())
}

/// A logical data-transfer session, materializable into either backend so the
/// same input can drive the Postgres and Trino reward paths.
struct DataSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    upload_bytes: u64,
    download_bytes: u64,
    rewardable_bytes: u64,
    num_dcs: u64,
    timestamp: DateTime<Utc>,
}

impl DataSession {
    async fn save_postgres(&self, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        data_session::HotspotDataSession {
            pub_key: self.pub_key.clone(),
            payer: self.payer.clone(),
            upload_bytes: self.upload_bytes as i64,
            download_bytes: self.download_bytes as i64,
            rewardable_bytes: self.rewardable_bytes as i64,
            num_dcs: self.num_dcs as i64,
            received_timestamp: self.timestamp,
            burn_timestamp: self.timestamp,
        }
        .save(txn)
        .await?;
        Ok(())
    }

    fn to_iceberg(&self) -> IcebergBurnedDataTransferSession {
        IcebergBurnedDataTransferSession {
            pub_key: self.pub_key.to_string(),
            payer: self.payer.to_string(),
            upload_bytes: self.upload_bytes,
            download_bytes: self.download_bytes,
            rewardable_bytes: self.rewardable_bytes,
            num_dcs: self.num_dcs,
            first_timestamp: self.timestamp.into(),
            last_timestamp: self.timestamp.into(),
            burn_timestamp: self.timestamp.into(),
        }
    }
}

/// The data sessions seeded by the reward tests. rewardable_bytes for the first
/// hotspot is intentionally lower than upload+download to prove rewardable_bytes
/// (not the byte sum) drives rewards.
fn data_sessions(ts: DateTime<Utc>) -> Vec<DataSession> {
    let timestamp = ts + ChronoDuration::hours(1);
    let upload_bytes = 1_024 * 1_000;
    let download_bytes = 1_024 * 50_000;
    vec![
        DataSession {
            pub_key: HOTSPOT_1.parse().unwrap(),
            payer: PAYER_1.parse().unwrap(),
            upload_bytes,
            download_bytes,
            rewardable_bytes: 1_024 * 1_000,
            num_dcs: 5_000_000,
            timestamp,
        },
        DataSession {
            pub_key: HOTSPOT_2.parse().unwrap(),
            payer: PAYER_1.parse().unwrap(),
            upload_bytes,
            download_bytes,
            rewardable_bytes: 1_024 * 1_000 + 1_024 * 50_000,
            num_dcs: 5_000_000,
            timestamp,
        },
        DataSession {
            pub_key: HOTSPOT_3.parse().unwrap(),
            payer: PAYER_1.parse().unwrap(),
            upload_bytes,
            download_bytes,
            rewardable_bytes: 1_024 * 1_000 + 1_024 * 50_000,
            num_dcs: 5_000_000,
            timestamp,
        },
    ]
}

/// Write data sessions into BOTH backends — Postgres and the Trino-backed
/// `data_transfer.burned_sessions` iceberg table. Tests read from whichever
/// backend they pass via the `DataSessionSource`.
async fn write_sessions(
    txn: &mut Transaction<'_, Postgres>,
    harness: &IcebergTestHarness,
    sessions: &[DataSession],
) -> anyhow::Result<()> {
    for session in sessions {
        session.save_postgres(txn).await?;
    }

    let rows = sessions
        .iter()
        .map(DataSession::to_iceberg)
        .collect::<Vec<_>>();
    harness
        .get_table_writer_in::<IcebergBurnedDataTransferSession>(
            burned_session::NAMESPACE,
            burned_session::TABLE_NAME,
        )
        .await?
        .write_idempotent("seed_data_sessions", rows)
        .await?;

    Ok(())
}

/// Seed the standard three-hotspot fixture into both backends and return the
/// total rewardable bytes.
async fn seed_data_sessions(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
    harness: &IcebergTestHarness,
) -> anyhow::Result<u64> {
    let sessions = data_sessions(ts);
    write_sessions(txn, harness, &sessions).await?;
    Ok(sessions.iter().map(|s| s.rewardable_bytes).sum())
}

/// One session per hotspot with explicit DC amounts (nominal bytes), for tests
/// that need to control demand and proportions.
fn data_sessions_with_dc(ts: DateTime<Utc>, dcs: &[(&str, u64)]) -> Vec<DataSession> {
    let timestamp = ts + ChronoDuration::hours(1);
    dcs.iter()
        .map(|(pubkey, num_dcs)| DataSession {
            pub_key: pubkey.parse().unwrap(),
            payer: PAYER_1.parse().unwrap(),
            upload_bytes: 0,
            download_bytes: 0,
            rewardable_bytes: 1_024,
            num_dcs: *num_dcs,
            timestamp,
        })
        .collect()
}
