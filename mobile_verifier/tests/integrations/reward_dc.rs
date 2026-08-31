//! Integration tests for the data-transfer reward path (`reward_dc`): data
//! transfer consumes the entire emissions pool and Proof-of-Coverage is not
//! rewarded. Each hotspot with burned data-transfer sessions splits the pool in
//! proportion to its data credits, with rounding dust emitted as a single
//! `UnallocatedReward`.
//!
//! Burned sessions are read from the Trino-backed
//! `data_transfer.burned_sessions` iceberg table — the only backend the reward
//! pipeline uses.

use crate::common::{self, default_price_info, reward_info_24_hours};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::data_transfer::burned_session::{
    self, IcebergBurnedDataTransferSession,
};
use mobile_verifier::rewarder::EpochRewardInfo;
use mobile_verifier::{reward_shares, rewarder};
use rust_decimal::prelude::*;

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const HOTSPOT_3: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const PAYER_1: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

#[tokio::test]
async fn test_dc_rewards() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    // seed burned data sessions into the Trino-backed iceberg table
    seed_data_sessions(reward_info.epoch_period.start, &harness).await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);

    // run data-transfer rewards, reading burned sessions from Trino
    rewarder::reward_dc(
        &trino,
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

#[tokio::test]
async fn test_no_data_sessions_unallocate_whole_pool() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    // The iceberg tables are created but no data sessions are seeded, so there is
    // nothing to distribute against.
    let harness = common::setup_iceberg().await?;
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);

    rewarder::reward_dc(
        &trino,
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

#[tokio::test]
async fn test_unequal_dc_rewards_proportionally() -> anyhow::Result<()> {
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
    write_sessions(&harness, &sessions).await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc(
        &trino,
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

#[tokio::test]
async fn test_oversubscribed_distributes_whole_pool() -> anyhow::Result<()> {
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
    write_sessions(&harness, &sessions).await?;

    let price_info = default_price_info();
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc(
        &trino,
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

#[tokio::test]
async fn test_single_hotspot_takes_whole_pool() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let harness = common::setup_iceberg().await?;

    let sessions = data_sessions_with_dc(reward_info.epoch_period.start, &[(HOTSPOT_1, 5_000_000)]);
    write_sessions(&harness, &sessions).await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc(
        &trino,
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

// Data transfer is the residual of `hnt_rewards_issued` after the flat
// service-provider cut — zero under HIP-150 — so the 3× cap (which moves HNT into
// delegation) and the backstop (which re-emits it) land entirely on the
// data-transfer pool. The two
// tests below drive a capped and a backstopped epoch end-to-end and assert the
// distributed pool shrinks / grows accordingly — the only place that wiring is
// exercised at the integration level (the split math itself is unit-tested in
// `reward_shares::emissions_split`). Expectations are concrete, computed
// independently of `hip_149_reward_pools` so they can't pass by mirroring a bug in
// the code under test.

/// A round 1e12-bone pool, so the data-transfer pools land on clean numbers.
const SPLIT_TEST_EMISSIONS: u64 = 1_000_000_000_000;
/// Baseline (6% delegation) data-transfer pool at [`SPLIT_TEST_EMISSIONS`]:
/// 94% issued (940e9), all of it (HIP-150: the SP allocation is contributed to
/// this pool). The cap shrinks below this; the backstop grows above it.
/// Pre-HIP-150 this was 700e9 — 940e9 less a 24% SP cut of 240e9.
const BASELINE_DATA_POOL: u64 = 940_000_000_000;

/// [`reward_info_24_hours`] with the on-chain split overridden. `hnt_issued` is what
/// the chain handed this rewarder; `delegation` is paid to veHNT holders on-chain.
/// Emissions are their sum, so only the issued/delegation split moves.
fn reward_info_with_split(hnt_issued: u64, delegation: u64) -> EpochRewardInfo {
    let mut reward_info = reward_info_24_hours();
    reward_info.epoch_emissions = Decimal::from(hnt_issued + delegation);
    reward_info.hnt_rewards_issued = Decimal::from(hnt_issued);
    reward_info.delegation_rewards_issued = Decimal::from(delegation);
    reward_info
}

/// Data-transfer pool computed *independently* of `hip_149_reward_pools` (the code
/// the production path uses).
///
/// HIP-150 Decision 3: the service-provider allocation is contributed to the
/// deployer pool, so data transfer is the entire issued HNT. Before HIP-150 this
/// subtracted a floored 24% of total emissions; when the contribution ends that
/// subtraction comes back.
fn expected_data_transfer_pool(reward_info: &EpochRewardInfo) -> u64 {
    reward_info.hnt_rewards_issued.to_u64().unwrap()
}

#[tokio::test]
async fn test_cap_shrinks_data_transfer_pool() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    // 3× cap moved 14% of emissions out of the data bucket into delegation:
    // delegation 6%+14%=20%, issued HNT 80%. Data transfer takes the whole issued
    // amount (HIP-150), so it absorbs the entire cut (94% → 80%).
    let reward_info = reward_info_with_split(
        SPLIT_TEST_EMISSIONS * 80 / 100,
        SPLIT_TEST_EMISSIONS * 20 / 100,
    );

    let harness = common::setup_iceberg().await?;
    seed_data_sessions(reward_info.epoch_period.start, &harness).await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc(
        &trino,
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
    assert_eq!(realized, 800_000_000_000, "80% issued, no SP cut");
    assert!(
        realized < BASELINE_DATA_POOL,
        "cap must shrink the data-transfer pool below baseline"
    );

    Ok(())
}

#[tokio::test]
async fn test_backstop_grows_data_transfer_pool() -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    // Backstop re-emitted HNT into the data bucket: issued HNT rises to 98%
    // (delegation 2%). Data transfer takes the whole issued amount (HIP-150), so
    // it absorbs the entire boost (94% → 98%).
    let reward_info = reward_info_with_split(
        SPLIT_TEST_EMISSIONS * 98 / 100,
        SPLIT_TEST_EMISSIONS * 2 / 100,
    );

    let harness = common::setup_iceberg().await?;
    seed_data_sessions(reward_info.epoch_period.start, &harness).await?;

    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    rewarder::reward_dc(
        &trino,
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
    assert_eq!(realized, 980_000_000_000, "98% issued, no SP cut");
    assert!(
        realized > BASELINE_DATA_POOL,
        "backstop must grow the data-transfer pool above baseline"
    );

    Ok(())
}

/// A burned data-transfer session, written into the Trino-backed
/// `data_transfer.burned_sessions` iceberg table to drive the reward path.
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
    fn to_iceberg(&self) -> IcebergBurnedDataTransferSession {
        IcebergBurnedDataTransferSession {
            pub_key: self.pub_key.to_string(),
            payer: self.payer.to_string(),
            upload_bytes: self.upload_bytes,
            download_bytes: self.download_bytes,
            rewardable_bytes: self.rewardable_bytes,
            num_dcs: self.num_dcs,
            // A row as written before HIP-150, so these also cover the
            // backward-compatible path: rewards distribute pro-rata of num_dcs
            // whether or not a multiplier produced it.
            multiplier: None,
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

/// Write data sessions into the Trino-backed `data_transfer.burned_sessions`
/// iceberg table — the sole backend the reward pipeline reads.
async fn write_sessions(
    harness: &IcebergTestHarness,
    sessions: &[DataSession],
) -> anyhow::Result<()> {
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

/// Seed the standard three-hotspot fixture and return the total rewardable bytes.
async fn seed_data_sessions(
    ts: DateTime<Utc>,
    harness: &IcebergTestHarness,
) -> anyhow::Result<u64> {
    let sessions = data_sessions(ts);
    write_sessions(harness, &sessions).await?;
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
