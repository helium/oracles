use crate::common::{self, MockFileSinkReceiver, MockHexBoostingClient};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use file_store::{
    coverage::{CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel},
    speedtest::CellSpeedtest,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, GatewayReward, HeartbeatValidity, RadioReward, SeniorityUpdateReason,
    SignalLevel, UnallocatedReward, UnallocatedRewardType,
};
use mobile_verifier::{
    cell_type::CellType,
    coverage::CoverageObject,
    data_session,
    heartbeats::{HbType, Heartbeat, ValidatedHeartbeat},
    reward_shares, rewarder, speedtests,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const HOTSPOT_3: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const PAYER_1: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

#[sqlx::test]
async fn test_poc_and_dc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let (speedtest_avg_client, _speedtest_avg_server) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(epoch.start, &mut txn).await?;
    seed_speedtests(epoch.end, &mut txn).await?;
    seed_data_sessions(epoch.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let boosted_hexes = vec![];

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
    if let Ok((poc_rewards, dc_rewards, unallocated_poc_reward)) = rewards {
        // assert poc reward outputs
        let hotspot_1_reward = 24_108_003_121_986;
        let hotspot_2_reward = 24_108_003_121_986;
        let hotspot_3_reward = 964_320_124_879;
        assert_eq!(hotspot_1_reward, poc_rewards[0].poc_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(poc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(hotspot_2_reward, poc_rewards[1].poc_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(poc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(hotspot_3_reward, poc_rewards[2].poc_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(poc_rewards[2].hotspot_key.clone()).to_string()
        );

        // assert the boosted hexes in the radio rewards
        // boosted hexes will contain the used multiplier for each boosted hex
        // in this test there are no boosted hexes
        assert_eq!(0, poc_rewards[0].boosted_hexes.len());
        assert_eq!(0, poc_rewards[1].boosted_hexes.len());
        assert_eq!(0, poc_rewards[2].boosted_hexes.len());

        // assert unallocated amount
        assert_eq!(
            UnallocatedRewardType::Poc as i32,
            unallocated_poc_reward.reward_type
        );
        assert_eq!(1, unallocated_poc_reward.amount);

        // assert the dc reward outputs
        assert_eq!(500_000, dc_rewards[0].dc_transfer_reward);
        assert_eq!(
            HOTSPOT_1.to_string(),
            PublicKeyBinary::from(dc_rewards[0].hotspot_key.clone()).to_string()
        );
        assert_eq!(500_000, dc_rewards[1].dc_transfer_reward);
        assert_eq!(
            HOTSPOT_3.to_string(),
            PublicKeyBinary::from(dc_rewards[1].hotspot_key.clone()).to_string()
        );
        assert_eq!(500_000, dc_rewards[2].dc_transfer_reward);
        assert_eq!(
            HOTSPOT_2.to_string(),
            PublicKeyBinary::from(dc_rewards[2].hotspot_key.clone()).to_string()
        );

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = poc_rewards.iter().map(|r| r.poc_reward).sum();
        let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
        let unallocated_sum: u64 = unallocated_poc_reward.amount;
        let total = poc_sum + dc_sum + unallocated_sum;

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
) -> anyhow::Result<(Vec<RadioReward>, Vec<GatewayReward>, UnallocatedReward)> {
    // get the filestore outputs from rewards run

    // expect 3 gateway rewards for dc transfer
    let dc_reward1 = mobile_rewards.receive_gateway_reward().await;
    let dc_reward2 = mobile_rewards.receive_gateway_reward().await;
    let dc_reward3 = mobile_rewards.receive_gateway_reward().await;
    let mut dc_rewards = vec![dc_reward1, dc_reward2, dc_reward3];
    dc_rewards.sort_by(|a, b| b.hotspot_key.cmp(&a.hotspot_key));

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

    Ok((poc_rewards, dc_rewards, unallocated_poc_reward))
}

async fn seed_heartbeats(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse().unwrap();
        let cbsd_id_1 = "P27-SCE4255W0001".to_string();
        let cov_obj_1 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            Some(cbsd_id_1.clone()),
            hotspot_key1.clone(),
            0x8a1fb466d2dffff_u64,
            true,
        );
        let cbrs_heartbeat_1 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Cbrs,
                hotspot_key: hotspot_key1,
                cbsd_id: Some(cbsd_id_1),
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_1.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
            },
            cell_type: CellType::SercommIndoor,
            distance_to_asserted: None,
            coverage_meta: None,
            location_trust_score_multiplier: dec!(1.0),
            validity: HeartbeatValidity::Valid,
        };

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse().unwrap();
        let cbsd_id_2 = "P27-SCE4255W0002".to_string();
        let cov_obj_2 = create_coverage_object(
            ts + ChronoDuration::hours(n),
            Some(cbsd_id_2.clone()),
            hotspot_key2.clone(),
            0x8a1fb49642dffff_u64,
            false,
        );
        let cbrs_heartbeat_2 = ValidatedHeartbeat {
            heartbeat: Heartbeat {
                hb_type: HbType::Cbrs,
                hotspot_key: hotspot_key2,
                cbsd_id: Some(cbsd_id_2),
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(cov_obj_2.coverage_object.uuid),
                location_validation_timestamp: None,
                timestamp: ts + ChronoDuration::hours(n),
            },
            cell_type: CellType::SercommOutdoor,
            distance_to_asserted: None,
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
        let wifi_heartbeat = ValidatedHeartbeat {
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

        save_seniority_object(ts + ChronoDuration::hours(n), &wifi_heartbeat, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &cbrs_heartbeat_1, txn).await?;
        save_seniority_object(ts + ChronoDuration::hours(n), &cbrs_heartbeat_2, txn).await?;

        cbrs_heartbeat_1.save(txn).await?;
        cbrs_heartbeat_2.save(txn).await?;
        wifi_heartbeat.save(txn).await?;

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

async fn seed_data_sessions(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let data_session_1 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        num_dcs: 5000000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };
    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_2.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        num_dcs: 5000000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };
    let data_session_3 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_3.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        num_dcs: 5000000,
        received_timestamp: ts + ChronoDuration::hours(1),
    };
    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    data_session_3.save(txn).await?;
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
    .bind(SeniorityUpdateReason::NewCoverageClaimTime as i32)
    .bind(hb.heartbeat.hb_type)
    .execute(&mut *exec)
    .await?;
    Ok(())
}
