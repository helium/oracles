//! Integration tests for the HIP-149 reward path (`reward_dc_hip_149`): data
//! transfer consumes the entire emissions pool and Proof-of-Coverage is not
//! rewarded. These mirror the seeding of the old `rewarder_poc_dc` tests
//! (heartbeats / speedtests / coverage / bans / unique connections) precisely to
//! prove the new path ignores every PoC input and only ever emits DC rewards
//! plus a single rounding-dust `UnallocatedReward`.

use std::ops::Range;

use crate::common::{self, default_price_info, reward_info_24_hours};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use file_store_oracles::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    speedtest::CellSpeedtest,
    unique_connections::{UniqueConnectionReq, UniqueConnectionsIngestReport},
};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::data_transfer::burned_session::{
    self, IcebergBurnedDataTransferSession,
};
use mobile_verifier::{
    cell_type::CellType,
    coverage::CoverageObject,
    data_session::{self, DataSessionSource},
    heartbeats::{Heartbeat, ValidatedHeartbeat},
    reward_shares, rewarder, speedtests, unique_connections,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        CoverageObjectValidity, HeartbeatValidity, LocationSource, SeniorityUpdateReason,
        SignalLevel,
    };
}

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
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
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
async fn test_poc_inputs_are_ignored(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap(); // wifi hotspot

    let harness = common::setup_iceberg().await?;

    // Seed every input that *would* earn coverage rewards on the old path:
    // heartbeats, speedtests, good oracle assignments, and a qualifying
    // unique-connections report — alongside the data sessions. The HIP-149 path
    // reads none of them, so a fully-qualified radio still earns no PoC.
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    let rewardable_total =
        seed_data_sessions(reward_info.epoch_period.start, &mut txn, &harness).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let mut txn = pool.begin().await?;
    seed_unique_connections(&mut txn, &[(pubkey.clone(), 42)], &reward_info.epoch_period).await?;
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

    // No coverage reward of any kind is emitted, despite fully-qualifying inputs.
    assert!(rewards.radio_reward_v2s.is_empty());
    assert!(rewards.radio_rewards.is_empty());

    // DC allocation is unaffected: all three hotspots are rewarded.
    let dc_rewards = &rewards.gateway_rewards;
    assert_eq!(dc_rewards.len(), 3);

    // rewardable_bytes (not upload + download) is what flows through to rewards.
    let rewardable_sum: u64 = dc_rewards.iter().map(|r| r.rewardable_bytes).sum();
    assert_eq!(rewardable_total, rewardable_sum);

    // The whole data-transfer pool is accounted for.
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
    assert_eq!(
        rewards.dc_transfer_sum() + rewards.unallocated_sum(),
        expected_sum
    );

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

    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
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
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
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
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
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
    let expected_sum = reward_shares::hip_149_reward_pools(&reward_info).data_transfer;
    assert_eq!(rewards.gateway_rewards.len(), 1);
    assert_eq!(rewards.gateway_rewards[0].dc_transfer_reward, expected_sum);
    assert_eq!(rewards.unallocated_sum(), 0);

    Ok(())
}

async fn seed_heartbeats(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();
        let cov_obj_1 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            hotspot_key1.clone(),
            0x8a1fb466d2dffff_u64,
            true,
        );
        let wifi_heartbeat_1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hotspot_key: hotspot_key1,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
                heartbeat_timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Gps,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
            asserted_location: None,
            device_type: None,
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: proto::HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            true,
        );
        let wifi_heartbeat_2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hotspot_key: hotspot_key2,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
                heartbeat_timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Gps,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
            asserted_location: None,
            device_type: None,
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: proto::HeartbeatValidity::Valid,
        };

        let hotspot_key3: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap();
        let cov_obj_3 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            hotspot_key3.clone(),
            0x8c2681a306607ff_u64,
            true,
        );
        let wifi_heartbeat_3 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hotspot_key: hotspot_key3,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                heartbeat_timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
            asserted_location: None,
            device_type: None,
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: proto::HeartbeatValidity::Valid,
        };

        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat_3, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat_1, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat_2, txn).await?;

        wifi_heartbeat_1.save(txn).await?;
        wifi_heartbeat_2.save(txn).await?;
        wifi_heartbeat_3.save(txn).await?;

        cov_obj_1.save(txn).await?;
        cov_obj_2.save(txn).await?;
        cov_obj_3.save(txn).await?;
    }
    Ok(())
}

async fn update_assignments(pool: &PgPool) -> anyhow::Result<()> {
    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_default(),
    )
    .await?;
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

        speedtests::save_speedtest(&hotspot1_speedtest, txn).await?;
        speedtests::save_speedtest(&hotspot2_speedtest, txn).await?;
        speedtests::save_speedtest(&hotspot3_speedtest, txn).await?;
    }
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

async fn seed_unique_connections(
    txn: &mut Transaction<'_, Postgres>,
    things: &[(PublicKeyBinary, u64)],
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    let mut reports = vec![];
    for (pubkey, unique_connections) in things {
        reports.push(UniqueConnectionsIngestReport {
            received_timestamp: epoch.start + chrono::Duration::hours(1),
            report: UniqueConnectionReq {
                pubkey: pubkey.clone(),
                start_timestamp: Utc::now(),
                end_timestamp: Utc::now(),
                unique_connections: *unique_connections,
                timestamp: Utc::now(),
                carrier_key: pubkey.clone(),
                signature: vec![],
            },
        });
    }
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
            signal_level: proto::SignalLevel::High,
            signal_power: 1000,
        }],
        indoor,
        trust_score: 1000,
        signature: Vec::new(),
    };
    CoverageObject {
        coverage_object: report,
        validity: proto::CoverageObjectValidity::Valid,
    }
}

//TODO: use existing save methods instead of manual sql
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
          ($1, $2, $3, $4, $5, $6, 'wifi'::radio_type)
        "#,
    )
    .bind(hb.heartbeat.key())
    .bind(hb.heartbeat.timestamp)
    .bind(hb.heartbeat.coverage_object)
    .bind(ts)
    .bind(ts)
    .bind(proto::SeniorityUpdateReason::NewCoverageClaimTime as i32)
    .execute(&mut **exec)
    .await?;
    Ok(())
}
