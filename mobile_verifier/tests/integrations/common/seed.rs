use crate::common;
use crate::rewarder_poc_dc::proto;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use file_store_oracles::coverage::{
    CoverageObject as FSCoverageObject, KeyType, RadioHexSignalLevel,
};
use file_store_oracles::speedtest::CellSpeedtest;
use file_store_oracles::unique_connections::{UniqueConnectionReq, UniqueConnectionsIngestReport};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::cell_type::CellType;
use mobile_verifier::coverage::CoverageObject;
use mobile_verifier::heartbeats::{HbType, Heartbeat, ValidatedHeartbeat};
use mobile_verifier::{data_session, speedtests, unique_connections};
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use std::ops::Range;
use uuid::Uuid;

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
pub const HOTSPOT_3: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
const PAYER_1: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

pub async fn seed_heartbeats(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot_key1: PublicKeyBinary = HOTSPOT_1.to_string().parse()?;
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

        let hotspot_key2: PublicKeyBinary = HOTSPOT_2.to_string().parse()?;
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

        let hotspot_key3: PublicKeyBinary = HOTSPOT_3.to_string().parse()?;
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

pub async fn update_assignments(pool: &PgPool) -> anyhow::Result<()> {
    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_default(),
    )
    .await?;
    Ok(())
}

pub async fn update_assignments_bad(pool: &PgPool) -> anyhow::Result<()> {
    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_bad(),
    )
    .await?;
    Ok(())
}

pub async fn seed_speedtests(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for n in 0..24 {
        let hotspot1_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_1.parse()?,
            serial: "serial1".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        let hotspot2_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_2.parse()?,
            serial: "serial2".to_string(),
            timestamp: ts - ChronoDuration::hours(n * 4),
            upload_speed: 100_000_000,
            download_speed: 100_000_000,
            latency: 50,
        };

        let hotspot3_speedtest = CellSpeedtest {
            pubkey: HOTSPOT_3.parse()?,
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

pub async fn seed_data_sessions(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<u64> {
    let rewardable_bytes_1 = 1_024 * 1_000;
    let data_session_1 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse()?,
        payer: PAYER_1.parse()?,
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        // Here to test that rewardable_bytes is the one taken into account we lower it
        rewardable_bytes: rewardable_bytes_1,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
        burn_timestamp: ts + ChronoDuration::hours(1),
    };

    let rewardable_bytes_2 = 1_024 * 1_000 + 1_024 * 50_000;
    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_2.parse()?,
        payer: PAYER_1.parse()?,
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        rewardable_bytes: rewardable_bytes_2,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
        burn_timestamp: ts + ChronoDuration::hours(1),
    };

    let rewardable_bytes_3 = 1_024 * 1_000 + 1_024 * 50_000;
    let data_session_3 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_3.parse()?,
        payer: PAYER_1.parse()?,
        upload_bytes: 1_024 * 1_000,
        download_bytes: 1_024 * 50_000,
        rewardable_bytes: rewardable_bytes_3,
        num_dcs: 5_000_000,
        received_timestamp: ts + ChronoDuration::hours(1),
        burn_timestamp: ts + ChronoDuration::hours(1),
    };
    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    data_session_3.save(txn).await?;

    let rewardable = rewardable_bytes_1 + rewardable_bytes_2 + rewardable_bytes_3;

    Ok(rewardable as u64)
}

pub async fn seed_unique_connections(
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
    .execute(&mut **exec)
    .await?;
    Ok(())
}
