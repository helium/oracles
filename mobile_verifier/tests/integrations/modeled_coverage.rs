use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::{CoverageObjectIngestReport, RadioHexSignalLevel},
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
    wifi_heartbeat::{WifiHeartbeat, WifiHeartbeatIngestReport},
};
use futures::stream::{self, StreamExt};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{CoverageObjectValidity, LocationSource, SignalLevel},
};
use hextree::Cell;
use mobile_config::{
    boosted_hex_info::{BoostedHexInfo, BoostedHexes},
    client::{authorization_client::AuthorizationVerifier, ClientError},
};

use mobile_verifier::{
    coverage::{CoverageClaimTimeCache, CoverageObject, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::{
        last_location::LocationCache, Heartbeat, HeartbeatReward, KeyType, ValidatedHeartbeat,
    },
    reward_shares::CoverageShares,
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    seniority::{Seniority, SeniorityUpdate},
    sp_boosted_rewards_bans::BannedRadios,
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    unique_connections::UniqueConnectionCounts,
};
use rust_decimal_macros::dec;
use solana_sdk::pubkey::Pubkey;
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroU32, ops::Range, pin::pin, str::FromStr, sync::Arc};
use uuid::Uuid;

use crate::common::{self, GatewayClientAllOwnersValid};

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
const BOOST_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

#[sqlx::test]
async fn test_save_wifi_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let cache = CoverageObjectCache::new(&pool);
    let uuid = Uuid::new_v4();
    let coverage_claim_time = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
    let key: PublicKeyBinary = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
        .parse()
        .unwrap();
    let key = KeyType::from(&key);

    assert!(cache.fetch_coverage_object(&uuid, key).await?.is_none());

    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(
            "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
        ),
        coverage_claim_time,
        coverage: vec![
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46622dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46632dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46642dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
        ],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co = CoverageObject {
        coverage_object: co,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co.save(&mut transaction).await?;

    // Test coverage claim time
    let cctc = CoverageClaimTimeCache::new();
    let expected_coverage_claim_time = cctc
        .fetch_coverage_claim_time(key, &Some(uuid), &mut transaction)
        .await?
        .unwrap();

    assert_eq!(expected_coverage_claim_time, coverage_claim_time);

    transaction.commit().await?;

    let coverage = cache.fetch_coverage_object(&uuid, key).await?.unwrap();
    assert_eq!(coverage.covered_hexes.len(), 3);

    Ok(())
}

#[sqlx::test]
async fn test_save_cbrs_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let cache = CoverageObjectCache::new(&pool);
    let uuid = Uuid::new_v4();
    let coverage_claim_time = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
    let key = "P27-SCE4255W120200039521XGB0103";
    let key = KeyType::from(key);

    assert!(cache.fetch_coverage_object(&uuid, key).await?.is_none());

    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(
            "P27-SCE4255W120200039521XGB0103".to_string(),
        ),
        coverage_claim_time,
        coverage: vec![
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46622dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46632dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46642dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
        ],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co = CoverageObject {
        coverage_object: co,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co.save(&mut transaction).await?;

    // Test coverage claim time
    let cctc = CoverageClaimTimeCache::new();
    let expected_coverage_claim_time = cctc
        .fetch_coverage_claim_time(key, &Some(uuid), &mut transaction)
        .await?
        .unwrap();

    assert_eq!(expected_coverage_claim_time, coverage_claim_time);

    transaction.commit().await?;

    let coverage = cache.fetch_coverage_object(&uuid, key).await?.unwrap();
    assert_eq!(coverage.covered_hexes.len(), 3);

    Ok(())
}

#[sqlx::test]
async fn test_coverage_object_save_updates(pool: PgPool) -> anyhow::Result<()> {
    let cache = CoverageObjectCache::new(&pool);
    let uuid = Uuid::new_v4();
    let coverage_claim_time = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();
    let key = "P27-SCE4255W120200039521XGB0103";
    let key = KeyType::from(key);

    assert!(cache.fetch_coverage_object(&uuid, key).await?.is_none());

    let co1 = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(
            "P27-SCE4255W120200039521XGB0103".to_string(),
        ),
        coverage_claim_time,
        coverage: vec![file_store::coverage::RadioHexSignalLevel {
            location: "8a1fb46622dffff".parse().unwrap(),
            signal_level: SignalLevel::High,
            signal_power: 1000,
        }],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co1 = CoverageObject {
        coverage_object: co1,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co1.save(&mut transaction).await?;
    transaction.commit().await?;

    let co2 = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(
            "P27-SCE4255W120200039521XGB0103".to_string(),
        ),
        coverage_claim_time,
        coverage: vec![file_store::coverage::RadioHexSignalLevel {
            location: "8a1fb46622dffff".parse().unwrap(),
            signal_level: SignalLevel::Low,
            signal_power: 5,
        }],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co2 = CoverageObject {
        coverage_object: co2,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co2.save(&mut transaction).await?;
    transaction.commit().await?;

    let new_signal_power: i32 = sqlx::query_scalar("SELECT signal_power FROM hexes")
        .fetch_one(&pool)
        .await?;

    assert_eq!(new_signal_power, 5);

    Ok(())
}

#[derive(Copy, Clone)]
struct AllPubKeysAuthed;

#[async_trait::async_trait]
impl AuthorizationVerifier for AllPubKeysAuthed {
    async fn verify_authorized_key(
        &self,
        _pub_key: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}

fn heartbeats<'a>(
    num: usize,
    start: DateTime<Utc>,
    hotspot_key: &'a PublicKeyBinary,
    cbsd_id: &'a str,
    lon: f64,
    lat: f64,
    coverage_object: Uuid,
) -> impl Iterator<Item = CbrsHeartbeatIngestReport> + 'a {
    (0..num).map(move |i| {
        let report = CbrsHeartbeat {
            pubkey: hotspot_key.clone(),
            lon,
            lat,
            operation_mode: true,
            cbsd_id: cbsd_id.to_string(),
            // Unused:
            hotspot_type: String::new(),
            cell_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            cbsd_category: String::new(),
            coverage_object: Vec::from(coverage_object.into_bytes()),
        };
        CbrsHeartbeatIngestReport {
            report,
            received_timestamp: start + Duration::hours(i as i64),
        }
    })
}

fn bytes_per_s(mbps: u64) -> u64 {
    mbps * 125000
}

fn acceptable_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
    Speedtest {
        report: CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed: bytes_per_s(10),
            download_speed: bytes_per_s(100),
            latency: 25,
            serial: "".to_string(),
        },
    }
}

fn degraded_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
    Speedtest {
        report: CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed: bytes_per_s(5),
            download_speed: bytes_per_s(60),
            latency: 60,
            serial: "".to_string(),
        },
    }
}

fn failed_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
    Speedtest {
        report: CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed: bytes_per_s(1),
            download_speed: bytes_per_s(20),
            latency: 110,
            serial: "".to_string(),
        },
    }
}

fn poor_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
    Speedtest {
        report: CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed: bytes_per_s(2),
            download_speed: bytes_per_s(40),
            latency: 90,
            serial: "".to_string(),
        },
    }
}

fn signal_level(hex: &str, signal_level: SignalLevel) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power: 0, // Unused
    })
}

fn signal_power(
    hex: &str,
    signal_level: SignalLevel,
    signal_power: i32,
) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power,
    })
}

async fn process_input(
    pool: &PgPool,
    epoch: &Range<DateTime<Utc>>,
    coverage_objs: impl Iterator<Item = CoverageObjectIngestReport>,
    heartbeats: impl Iterator<Item = CbrsHeartbeatIngestReport>,
) -> anyhow::Result<()> {
    let coverage_objects = CoverageObjectCache::new(pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();
    let location_cache = LocationCache::new(pool);

    let mut transaction = pool.begin().await?;
    let mut coverage_objs = pin!(CoverageObject::validate_coverage_objects(
        Arc::new(AllPubKeysAuthed),
        stream::iter(coverage_objs)
    ));
    while let Some(coverage_obj) = coverage_objs.next().await.transpose()? {
        coverage_obj.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    let _ = common::set_unassigned_oracle_boosting_assignments(
        pool,
        &common::mock_hex_boost_data_default(),
    )
    .await?;

    let mut transaction = pool.begin().await?;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        stream::iter(heartbeats.map(Heartbeat::from)),
        Arc::new(GatewayClientAllOwnersValid),
        &coverage_objects,
        &location_cache,
        2000,
        epoch,
        Arc::new(MockGeofence),
    ));
    while let Some(heartbeat) = heartbeats.next().await.transpose()? {
        let coverage_claim_time = coverage_claim_time_cache
            .fetch_coverage_claim_time(
                heartbeat.heartbeat.key(),
                &heartbeat.heartbeat.coverage_object,
                &mut transaction,
            )
            .await?;
        let latest_seniority =
            Seniority::fetch_latest(heartbeat.heartbeat.key(), &mut transaction).await?;
        let seniority_update = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time.unwrap(),
            latest_seniority,
        )?;
        seniority_update.execute(&mut transaction).await?;
        heartbeat.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    Ok(())
}

#[sqlx::test]
async fn scenario_one(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let coverage_object = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
            coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    process_input(
        &pool,
        &(start..end),
        vec![coverage_object].into_iter(),
        heartbeats(13, start, &owner, &cbsd_id, 0.0, 0.0, uuid),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        acceptable_speedtest(owner.clone(), last_timestamp),
        acceptable_speedtest(owner.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner.clone(), SpeedtestAverage::from(owner_speedtests));
    let speedtest_avgs = SpeedtestAverages { averages };

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner, Some(cbsd_id))),
        dec!(250)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_two(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let cbsd_id_1 = "P27-SCE4255W120200039521XGB0103".to_string();
    let cbsd_id_2 = "P27-SCE4255W120200039521XGB0104".to_string();

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_1.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };
    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_2.clone()),
            coverage_claim_time: "2022-01-31 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a30641dff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?, // Second hex is shared
                signal_level("8c2681a3066a9ff", SignalLevel::Low)?,
                signal_level("8c2681a306607ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e9ff", SignalLevel::Low)?,
                signal_level("8c2681a306481ff", SignalLevel::Low)?,
                signal_level("8c2681a302991ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let owner_1: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let owner_2: PublicKeyBinary = "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR".parse()?;

    let heartbeats_1 = heartbeats(13, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(13, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);

    process_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        degraded_speedtest(owner_1.clone(), last_timestamp),
        degraded_speedtest(owner_1.clone(), end),
    ];
    let speedtests_2 = vec![
        acceptable_speedtest(owner_2.clone(), last_timestamp),
        acceptable_speedtest(owner_2.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(owner_2.clone(), SpeedtestAverage::from(speedtests_2));
    let speedtest_avgs = SpeedtestAverages { averages };

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_1, Some(cbsd_id_1))),
        dec!(112.5)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_2, Some(cbsd_id_2))),
        dec!(250)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_three(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();
    let uuid_4 = Uuid::new_v4();
    let uuid_5 = Uuid::new_v4();
    let uuid_6 = Uuid::new_v4();

    let cbsd_id_1 = "P27-SCE4255W120200039521XGB0105".to_string();
    let cbsd_id_2 = "P27-SCE4255W120200039521XGB0106".to_string();
    let cbsd_id_3 = "P27-SCE4255W120200039521XGB0107".to_string();
    let cbsd_id_4 = "P27-SCE4255W120200039521XGB0108".to_string();
    let cbsd_id_5 = "P27-SCE4255W120200039521XGB0109".to_string();
    let cbsd_id_6 = "P27-SCE4255W120200039521XGB0110".to_string();

    // All coverage objects share the same hexes

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_1.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_2.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_3 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_3,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_3.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_4 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_4,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_4.clone()),
            coverage_claim_time: "2022-01-31 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_5 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_5,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_5.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_6 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_6,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_6.clone()),
            coverage_claim_time: "2022-02-02 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let owner_1: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let owner_2: PublicKeyBinary = "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR".parse()?;
    let owner_3: PublicKeyBinary = "11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1".parse()?;
    let owner_4: PublicKeyBinary = "11Kgx4nqN7VpZXobEubLaTvJWkzHf1w1SrUUYK1CTiLFPgiQRHW".parse()?;
    let owner_5: PublicKeyBinary = "11Bn2erjB83zdCBrE248pTVBpTXSuN8Lur4v4mWFnf5Rpd8XK7n".parse()?;
    let owner_6: PublicKeyBinary = "11d5KySrfiMgaDoZ7B5CDm3meE1gQhUJ5EHuJvzwiWjdSUGhBsZ".parse()?;

    let heartbeats_1 = heartbeats(13, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(13, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);
    let heartbeats_3 = heartbeats(13, start, &owner_3, &cbsd_id_3, 0.0, 0.0, uuid_3);
    let heartbeats_4 = heartbeats(13, start, &owner_4, &cbsd_id_4, 0.0, 0.0, uuid_4);
    let heartbeats_5 = heartbeats(13, start, &owner_5, &cbsd_id_5, 0.0, 0.0, uuid_5);
    let heartbeats_6 = heartbeats(13, start, &owner_6, &cbsd_id_6, 0.0, 0.0, uuid_6);

    process_input(
        &pool,
        &(start..end),
        vec![
            coverage_object_1,
            coverage_object_2,
            coverage_object_3,
            coverage_object_4,
            coverage_object_5,
            coverage_object_6,
        ]
        .into_iter(),
        heartbeats_1
            .chain(heartbeats_2)
            .chain(heartbeats_3)
            .chain(heartbeats_4)
            .chain(heartbeats_5)
            .chain(heartbeats_6),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        poor_speedtest(owner_1.clone(), last_timestamp),
        poor_speedtest(owner_1.clone(), end),
    ];
    let speedtests_2 = vec![
        poor_speedtest(owner_2.clone(), last_timestamp),
        poor_speedtest(owner_2.clone(), end),
    ];
    let speedtests_3 = vec![
        acceptable_speedtest(owner_3.clone(), last_timestamp),
        acceptable_speedtest(owner_3.clone(), end),
    ];
    let speedtests_4 = vec![
        acceptable_speedtest(owner_4.clone(), last_timestamp),
        acceptable_speedtest(owner_4.clone(), end),
    ];
    let speedtests_5 = vec![
        failed_speedtest(owner_5.clone(), last_timestamp),
        failed_speedtest(owner_5.clone(), end),
    ];
    let speedtests_6 = vec![
        acceptable_speedtest(owner_6.clone(), last_timestamp),
        acceptable_speedtest(owner_6.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(owner_2.clone(), SpeedtestAverage::from(speedtests_2));
    averages.insert(owner_3.clone(), SpeedtestAverage::from(speedtests_3));
    averages.insert(owner_4.clone(), SpeedtestAverage::from(speedtests_4));
    averages.insert(owner_5.clone(), SpeedtestAverage::from(speedtests_5));
    averages.insert(owner_6.clone(), SpeedtestAverage::from(speedtests_6));
    let speedtest_avgs = SpeedtestAverages { averages };

    let mut boosted_hexes = BoostedHexes::default();
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8a1fb466d2dffff)?,
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb466d2dffff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(1).unwrap()],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8a1fb49642dffff)?,
        BoostedHexInfo {
            location: Cell::from_raw(0x8a1fb49642dffff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(2).unwrap()],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8c2681a306607ff)?,
        BoostedHexInfo {
            // hotspot 1's location
            location: Cell::from_raw(0x8c2681a306607ff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(3).unwrap()],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: Pubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &boosted_hexes,
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_1, Some(cbsd_id_1))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_2, Some(cbsd_id_2))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_3, Some(cbsd_id_3))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_4, Some(cbsd_id_4))),
        dec!(250)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_5, Some(cbsd_id_5))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_6, Some(cbsd_id_6))),
        dec!(0)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_four(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let coverage_object = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
            coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_power("8c2681a3064d9ff", SignalLevel::High, -9400)?,
                signal_power("8c2681a3065d3ff", SignalLevel::High, -9400)?,
                signal_power("8c2681a306635ff", SignalLevel::Medium, -10000)?,
                signal_power("8c2681a3066e7ff", SignalLevel::Medium, -10000)?,
                signal_power("8c2681a3065adff", SignalLevel::Medium, -10000)?,
                signal_power("8c2681a339a4bff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a3065d7ff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a306481ff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a30648bff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a30646bff", SignalLevel::Low, -11000)?,
            ],
            trust_score: 1000,
        },
    };
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    process_input(
        &pool,
        &(start..end),
        vec![coverage_object].into_iter(),
        heartbeats(13, start, &owner, &cbsd_id, 0.0, 0.0, uuid),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        acceptable_speedtest(owner.clone(), last_timestamp),
        acceptable_speedtest(owner.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner.clone(), SpeedtestAverage::from(owner_speedtests));
    let speedtest_avgs = SpeedtestAverages { averages };

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner, Some(cbsd_id))),
        dec!(19)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_five(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let cbsd_id_1 = "P27-SCE4255W120200039521XGB0103".to_string();
    let cbsd_id_2 = "P27-SCE4255W120200039521XGB0104".to_string();

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_1.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_power("8c2681a302991ff", SignalLevel::High, -9400)?,
                signal_power("8c2681a306601ff", SignalLevel::High, -9400)?,
                signal_power("8c2681a306697ff", SignalLevel::High, -9400)?,
                signal_power("8c2681a3028a7ff", SignalLevel::Medium, -10000)?,
                signal_power("8c2681a3064c1ff", SignalLevel::Medium, -10000)?,
                signal_power("8c2681a30671bff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a306493ff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a30659dff", SignalLevel::Low, -11000)?,
            ],
            trust_score: 1000,
        },
    };
    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_2.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_power("8c2681a3066abff", SignalLevel::High, -9400)?,
                signal_power("8c2681a3028a7ff", SignalLevel::Medium, -10500)?, // Second hex is shared
                signal_power("8c2681a3066a9ff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a3066a5ff", SignalLevel::Low, -11000)?,
                signal_power("8c2681a30640dff", SignalLevel::Low, -11000)?,
            ],
            trust_score: 1000,
        },
    };

    let owner_1: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let owner_2: PublicKeyBinary = "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR".parse()?;

    let heartbeats_1 = heartbeats(13, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(13, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);

    process_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        degraded_speedtest(owner_1.clone(), last_timestamp),
        degraded_speedtest(owner_1.clone(), end),
    ];
    let speedtests_2 = vec![
        acceptable_speedtest(owner_2.clone(), last_timestamp),
        acceptable_speedtest(owner_2.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(owner_2.clone(), SpeedtestAverage::from(speedtests_2));
    let speedtest_avgs = SpeedtestAverages { averages };

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_1, Some(cbsd_id_1))),
        dec!(19) * dec!(0.5)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_2, Some(cbsd_id_2))),
        dec!(8)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_six(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    // This is the same as scenario three, but with an acceptable speedtest
    // the sixth radio is not awarded

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();
    let uuid_4 = Uuid::new_v4();
    let uuid_5 = Uuid::new_v4();
    let uuid_6 = Uuid::new_v4();

    let cbsd_id_1 = "P27-SCE4255W120200039521XGB0105".to_string();
    let cbsd_id_2 = "P27-SCE4255W120200039521XGB0106".to_string();
    let cbsd_id_3 = "P27-SCE4255W120200039521XGB0107".to_string();
    let cbsd_id_4 = "P27-SCE4255W120200039521XGB0108".to_string();
    let cbsd_id_5 = "P27-SCE4255W120200039521XGB0109".to_string();
    let cbsd_id_6 = "P27-SCE4255W120200039521XGB0110".to_string();

    // All coverage objects share the same hexes

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_1.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_2.clone()),
            coverage_claim_time: "2022-01-31 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_3 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_3,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_3.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_4 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_4,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_4.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_5 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_5,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_5.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_6 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_6,
            key_type: file_store::coverage::KeyType::CbsdId(cbsd_id_6.clone()),
            coverage_claim_time: "2022-02-02 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,
                signal_level("8c2681a3065adff", SignalLevel::Low)?,
                signal_level("8c2681a339a4bff", SignalLevel::Low)?,
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let owner_1: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let owner_2: PublicKeyBinary = "11PGVtgW9aM9ynfvns5USUsynYQ7EsMpxVqWuDKqFogKQX7etkR".parse()?;
    let owner_3: PublicKeyBinary = "11ibmJmQXTL6qMh4cq9pJ7tUtrpafWaVjjT6qhY7CNvjyvY9g1".parse()?;
    let owner_4: PublicKeyBinary = "11Kgx4nqN7VpZXobEubLaTvJWkzHf1w1SrUUYK1CTiLFPgiQRHW".parse()?;
    let owner_5: PublicKeyBinary = "11Bn2erjB83zdCBrE248pTVBpTXSuN8Lur4v4mWFnf5Rpd8XK7n".parse()?;
    let owner_6: PublicKeyBinary = "11d5KySrfiMgaDoZ7B5CDm3meE1gQhUJ5EHuJvzwiWjdSUGhBsZ".parse()?;

    let heartbeats_1 = heartbeats(13, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(13, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);
    let heartbeats_3 = heartbeats(13, start, &owner_3, &cbsd_id_3, 0.0, 0.0, uuid_3);
    let heartbeats_4 = heartbeats(13, start, &owner_4, &cbsd_id_4, 0.0, 0.0, uuid_4);
    let heartbeats_5 = heartbeats(13, start, &owner_5, &cbsd_id_5, 0.0, 0.0, uuid_5);
    let heartbeats_6 = heartbeats(13, start, &owner_6, &cbsd_id_6, 0.0, 0.0, uuid_6);

    process_input(
        &pool,
        &(start..end),
        vec![
            coverage_object_1,
            coverage_object_2,
            coverage_object_3,
            coverage_object_4,
            coverage_object_5,
            coverage_object_6,
        ]
        .into_iter(),
        heartbeats_1
            .chain(heartbeats_2)
            .chain(heartbeats_3)
            .chain(heartbeats_4)
            .chain(heartbeats_5)
            .chain(heartbeats_6),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        poor_speedtest(owner_1.clone(), last_timestamp),
        poor_speedtest(owner_1.clone(), end),
    ];
    let speedtests_2 = vec![
        poor_speedtest(owner_2.clone(), last_timestamp),
        poor_speedtest(owner_2.clone(), end),
    ];
    let speedtests_3 = vec![
        acceptable_speedtest(owner_3.clone(), last_timestamp),
        acceptable_speedtest(owner_3.clone(), end),
    ];
    let speedtests_4 = vec![
        acceptable_speedtest(owner_4.clone(), last_timestamp),
        acceptable_speedtest(owner_4.clone(), end),
    ];
    let speedtests_5 = vec![
        acceptable_speedtest(owner_5.clone(), last_timestamp),
        acceptable_speedtest(owner_5.clone(), end),
    ];
    let speedtests_6 = vec![
        acceptable_speedtest(owner_6.clone(), last_timestamp),
        acceptable_speedtest(owner_6.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(owner_2.clone(), SpeedtestAverage::from(speedtests_2));
    averages.insert(owner_3.clone(), SpeedtestAverage::from(speedtests_3));
    averages.insert(owner_4.clone(), SpeedtestAverage::from(speedtests_4));
    averages.insert(owner_5.clone(), SpeedtestAverage::from(speedtests_5));
    averages.insert(owner_6.clone(), SpeedtestAverage::from(speedtests_6));
    let speedtest_avgs = SpeedtestAverages { averages };

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_1, Some(cbsd_id_1))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_2, Some(cbsd_id_2))),
        dec!(62.5)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_3, Some(cbsd_id_3))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_4, Some(cbsd_id_4))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_5, Some(cbsd_id_5))),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner_6, Some(cbsd_id_6))),
        dec!(0)
    );

    Ok(())
}

#[sqlx::test]
async fn ensure_lower_trust_score_for_distant_heartbeats(pool: PgPool) -> anyhow::Result<()> {
    let owner_1: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9"
        .parse()
        .unwrap();
    let coverage_object_uuid = Uuid::new_v4();

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid: coverage_object_uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(owner_1.clone()),
        coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
        trust_score: 1000,
    };

    let coverage_object =
        CoverageObject::validate(coverage_object, Arc::new(AllPubKeysAuthed)).await?;

    let mut transaction = pool.begin().await?;
    coverage_object.save(&mut transaction).await?;
    transaction.commit().await?;

    let max_covered_distance = 5_000;
    let coverage_object_cache = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let mk_heartbeat = |latlng: LatLng| WifiHeartbeatIngestReport {
        report: WifiHeartbeat {
            pubkey: owner_1.clone(),
            lon: latlng.lng(),
            lat: latlng.lat(),
            timestamp: DateTime::<Utc>::MIN_UTC,
            location_validation_timestamp: Some(DateTime::<Utc>::MIN_UTC),
            operation_mode: true,
            coverage_object: Vec::from(coverage_object_uuid.into_bytes()),
            location_source: LocationSource::Skyhook,
        },
        received_timestamp: Utc::now(),
    };

    let validate = |latlng: LatLng| {
        ValidatedHeartbeat::validate(
            mk_heartbeat(latlng).into(),
            Arc::new(GatewayClientAllOwnersValid),
            &coverage_object_cache,
            &location_cache,
            max_covered_distance,
            &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
            Arc::new(MockGeofence),
        )
    };

    let covered_cell_index: CellIndex = "8c2681a3064d9ff".parse()?;
    let covered_latlng = LatLng::from(covered_cell_index);

    // Constrain distances by only moving vertically
    let near_latlng = LatLng::new(40.0194278140, -105.272)?; // 35m
    let med_latlng = LatLng::new(40.0194278140, -105.274)?; // 205m
    let far_latlng = LatLng::new(40.0194278140, -105.3)?; // 2,419m
    let past_latlng = LatLng::new(40.0194278140, 105.2715848904)?; // 10,591,975m

    // It's easy to gloss over floats, let make sure the distances are within the ranges we expect.
    assert!((0.0..=200.0).contains(&covered_latlng.distance_m(near_latlng))); // Indoor low distance <= 200
    assert!((200.0..=300.0).contains(&covered_latlng.distance_m(med_latlng))); // Indoor Medium distance <= 300
    assert!(covered_latlng.distance_m(far_latlng) > 300.0); // Indoor Over Distance => 300
    assert!(covered_latlng.distance_m(past_latlng) > max_covered_distance as f64); // Indoor past max distance => max_distance

    let low_dist_validated = validate(near_latlng).await?;
    let med_dist_validated = validate(med_latlng).await?;
    let far_dist_validated = validate(far_latlng).await?;
    let past_dist_validated = validate(past_latlng).await?;

    assert_eq!(
        low_dist_validated.location_trust_score_multiplier,
        dec!(1.0)
    );
    assert_eq!(
        med_dist_validated.location_trust_score_multiplier,
        dec!(0.25)
    );
    assert_eq!(
        far_dist_validated.location_trust_score_multiplier,
        dec!(0.00)
    );
    assert_eq!(
        past_dist_validated.location_trust_score_multiplier,
        dec!(0.00)
    );

    Ok(())
}
