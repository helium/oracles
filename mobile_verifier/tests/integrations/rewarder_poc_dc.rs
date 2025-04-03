use std::ops::Range;

use crate::common::{
    self, default_price_info, default_rewards_info, MockHexBoostingClient, RadioRewardV2Ext,
    EMISSIONS_POOL_IN_BONES_24_HOURS,
};
use chrono::{DateTime, Duration as ChronoDuration, Duration, Utc};
use file_store::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    mobile_ban,
    speedtest::CellSpeedtest,
    unique_connections::{UniqueConnectionReq, UniqueConnectionsIngestReport},
};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::{
    banning,
    cell_type::CellType,
    coverage::CoverageObject,
    data_session,
    heartbeats::{HbType, Heartbeat, ValidatedHeartbeat},
    reward_shares, rewarder,
    sp_boosted_rewards_bans::{self, BannedRadioReport},
    speedtests, unique_connections,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        service_provider_boosted_rewards_banned_radio_req_v1::KeyType,
        service_provider_boosted_rewards_banned_radio_req_v1::{
            SpBoostedRewardsBannedRadioBanType, SpBoostedRewardsBannedRadioReason,
        },
        CoverageObjectValidity, HeartbeatValidity, LocationSource, SeniorityUpdateReason,
        ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        ServiceProviderBoostedRewardsBannedRadioReqV1, SignalLevel,
    };
}

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const HOTSPOT_3: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const PAYER_1: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

#[sqlx::test]
async fn test_poc_and_dc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info,
    )
    .await?;
    drop(mobile_rewards_client);

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2;
    let dc_rewards = rewards.gateway_reward;
    let unallocated_reward = rewards.unallocated.first();

    let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();

    assert_eq!(poc_sum / 3, poc_rewards[0].total_poc_reward());
    assert_eq!(poc_sum / 3, poc_rewards[1].total_poc_reward());
    assert_eq!(poc_sum / 3, poc_rewards[2].total_poc_reward());

    // assert the unallocated reward
    let unallocated_reward = unallocated_reward.unwrap();
    assert_eq!(unallocated_reward.amount, 1);

    // assert the boosted hexes in the radio rewards
    // boosted hexes will contain the used multiplier for each boosted hex
    // in this test there are no boosted hexes
    assert_eq!(0, poc_rewards[0].boosted_hexes_len());
    assert_eq!(0, poc_rewards[1].boosted_hexes_len());
    assert_eq!(0, poc_rewards[2].boosted_hexes_len());

    // assert the dc reward outputs
    assert_eq!(500_000, dc_rewards[0].dc_transfer_reward);
    assert_eq!(500_000, dc_rewards[1].dc_transfer_reward);
    assert_eq!(500_000, dc_rewards[2].dc_transfer_reward);

    // confirm the total rewards allocated matches expectations
    let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
    let total = poc_sum + dc_sum + unallocated_reward.amount;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(total) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.6));

    Ok(())
}

#[sqlx::test]
async fn test_qualified_wifi_poc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap(); // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    let rewardable_total = seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments_bad(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let boosted_hexes = vec![];
    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let price_info = default_price_info();

    // seed single unique conections report within epoch
    let mut txn = pool.begin().await?;
    seed_unique_connections(&mut txn, &[(pubkey.clone(), 42)], &reward_info.epoch_period).await?;
    txn.commit().await?;

    // SP ban radio, unique connections should supersede banning
    let mut txn = pool.begin().await?;
    ban_wifi_radio_for_epoch(&mut txn, pubkey.clone(), &reward_info.epoch_period).await?;
    txn.commit().await?;

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info,
    )
    .await?;
    drop(mobile_rewards_client);

    let msgs = mobile_rewards.finish().await?;
    let poc_rewards = msgs.radio_reward_v2;
    let dc_rewards = msgs.gateway_reward;

    // expecting single radio with poc rewards, no unallocated
    assert_eq!(poc_rewards.len(), 1);
    assert_eq!(dc_rewards.len(), 3);
    assert_eq!(msgs.unallocated.len(), 0);

    // Check that we used rewardable_bytes for calculation and not upload_bytes + download_bytes anymore
    let rewardable_sum: u64 = dc_rewards.iter().map(|r| r.rewardable_bytes).sum();
    assert_eq!(rewardable_total, rewardable_sum);

    // Check that we used rewardable_bytes for calculation and not upload_bytes + download_bytes anymore
    let rewardable_sum: u64 = dc_rewards.iter().map(|r| r.rewardable_bytes).sum();
    assert_eq!(rewardable_total, rewardable_sum);

    let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();
    let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
    let total = poc_sum + dc_sum;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    Ok(())
}

#[sqlx::test]
async fn test_sp_banned_radio(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap(); // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let boosted_hexes = vec![];
    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let price_info = default_price_info();

    let _rewarder = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info.clone(),
    )
    .await?;
    drop(mobile_rewards_client);

    let msgs = mobile_rewards.finish().await?;
    assert_eq!(msgs.gateway_reward.len(), 3);
    assert_eq!(msgs.radio_reward_v2.len(), 3);

    // ==============================================================
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    // SP ban radio, zeroed rewards are filtered out
    let mut txn = pool.begin().await?;
    ban_wifi_radio_for_epoch(&mut txn, pubkey.clone(), &reward_info.epoch_period).await?;
    txn.commit().await?;

    let _rewarder = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info,
    )
    .await?;
    drop(mobile_rewards_client);

    let msgs = mobile_rewards.finish().await?;
    assert_eq!(msgs.gateway_reward.len(), 3);
    assert_eq!(msgs.radio_reward_v2.len(), 2);

    Ok(())
}

#[sqlx::test]
async fn test_all_banned_radio(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap(); // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // ban radio
    let mut txn = pool.begin().await?;
    ban_radio(
        &mut txn,
        pubkey.clone(),
        mobile_ban::BanType::All,
        reward_info.rewards_issued_at,
    )
    .await?;
    txn.commit().await?;

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info,
    )
    .await?;
    drop(mobile_rewards_client);

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2;

    let dc_rewards = rewards.gateway_reward;

    // expecting single radio with poc rewards, no unallocated
    assert_eq!(poc_rewards.len(), 2);
    assert_eq!(dc_rewards.len(), 3);
    assert_eq!(rewards.unallocated.len(), 0);

    Ok(())
}

#[sqlx::test]
async fn test_data_banned_radio_still_receives_poc(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_nonblocking_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();

    let reward_info = default_rewards_info(EMISSIONS_POOL_IN_BONES_24_HOURS, Duration::hours(24));

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse().unwrap(); // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // ban radio for Data only
    let mut txn = pool.begin().await?;
    ban_radio(
        &mut txn,
        pubkey.clone(),
        mobile_ban::BanType::Data,
        reward_info.rewards_issued_at,
    )
    .await?;
    txn.commit().await?;

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &speedtest_avg_client,
        &reward_info,
        price_info,
    )
    .await?;
    drop(mobile_rewards_client);

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2;
    let dc_rewards = rewards.gateway_reward;

    assert_eq!(poc_rewards.len(), 3);
    assert_eq!(dc_rewards.len(), 0);
    assert_eq!(rewards.unallocated.len(), 1);

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
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key1,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Gps,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
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
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key2,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Gps,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
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
                hb_type: HbType::Wifi,
                hotspot_key: hotspot_key3,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_3.coverage_object.uuid),
                location_validation_timestamp: Some(ts - ChronoDuration::hours(24)),
                timestamp: ts + ChronoDuration::hours(n),
                location_source: proto::LocationSource::Skyhook,
            },
            cell_type: CellType::NovaGenericWifiIndoor,
            distance_to_asserted: Some(0),
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

async fn update_assignments_bad(pool: &PgPool) -> anyhow::Result<()> {
    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_bad(),
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

async fn seed_data_sessions(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<u64> {
    let rewardable_bytes_1 = 1_024 * 1_000;
    let data_session_1 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        // Here to test that rewardable_bytes is the one taken into account we lower it
        rewardable_bytes: rewardable_bytes_1,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };

    let rewardable_bytes_2 = 1_024 * 1_000 + 1_024 * 50_000;
    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_2.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        rewardable_bytes: rewardable_bytes_2,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };

    let rewardable_bytes_3 = 1_024 * 1_000 + 1_024 * 50_000;
    let data_session_3 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_3.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        rewardable_bytes: rewardable_bytes_3,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };
    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    data_session_3.save(txn).await?;

    let rewardable = rewardable_bytes_1 + rewardable_bytes_2 + rewardable_bytes_3;

    Ok(rewardable as u64)
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
          ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(hb.heartbeat.key())
    .bind(hb.heartbeat.timestamp)
    .bind(hb.heartbeat.coverage_object)
    .bind(ts)
    .bind(ts)
    .bind(proto::SeniorityUpdateReason::NewCoverageClaimTime as i32)
    .bind(hb.heartbeat.hb_type)
    .execute(&mut *exec)
    .await?;
    Ok(())
}

async fn ban_wifi_radio_for_epoch(
    txn: &mut Transaction<'_, Postgres>,
    pubkey: PublicKeyBinary,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    let until = (epoch.start + chrono::Duration::days(7)).timestamp_millis() as u64;
    let received_timestamp = (epoch.start + chrono::Duration::hours(2)).timestamp_millis() as u64;

    let ban_report = proto::ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
        received_timestamp,
        report: Some(proto::ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: pubkey.clone().into(),
            reason: proto::SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation.into(),
            until,
            signature: vec![],
            ban_type: proto::SpBoostedRewardsBannedRadioBanType::Poc.into(),
            key_type: Some(proto::KeyType::HotspotKey(pubkey.into())),
        }),
    };
    let ban_report = BannedRadioReport::try_from(ban_report)?;
    sp_boosted_rewards_bans::db::update_report(txn, &ban_report).await?;
    Ok(())
}

async fn ban_radio(
    txn: &mut Transaction<'_, Postgres>,
    pubkey: PublicKeyBinary,
    ban_type: mobile_ban::BanType,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    use file_store::mobile_ban;
    banning::db::update_hotspot_ban(
        txn,
        &mobile_ban::VerifiedBanReport {
            verified_timestamp: timestamp,
            report: mobile_ban::BanReport {
                received_timestamp: timestamp,
                report: mobile_ban::BanRequest {
                    hotspot_pubkey: pubkey.clone(),
                    timestamp,
                    ban_key: pubkey,
                    signature: vec![],
                    ban_action: mobile_ban::BanAction::Ban(mobile_ban::BanDetails {
                        hotspot_serial: "test-serial".to_string(),
                        message: "test-notes".to_string(),
                        reason: mobile_ban::BanReason::LocationGaming,
                        ban_type,
                        expiration_timestamp: None,
                    }),
                },
            },
            status: mobile_ban::VerifiedBanIngestReportStatus::Valid,
        },
    )
    .await?;
    Ok(())
}
