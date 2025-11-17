use chrono::{DateTime, Duration, Utc};
use file_store_oracles::{
    coverage::{CoverageObjectIngestReport, RadioHexSignalLevel},
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
    client::ClientError,
};

use mobile_verifier::{
    banning::BannedRadios,
    coverage::{CoverageClaimTimeCache, CoverageObject, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::{
        last_location::LocationCache, Heartbeat, HeartbeatReward, KeyType, ValidatedHeartbeat,
    },
    reward_shares::CoverageShares,
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    seniority::{Seniority, SeniorityUpdate},
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    unique_connections::UniqueConnectionCounts,
    IsAuthorized,
};
use rust_decimal_macros::dec;
use solana::SolPubkey;
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroU32, ops::Range, pin::pin, str::FromStr};
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

    let co = file_store_oracles::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store_oracles::coverage::KeyType::HotspotKey(
            "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
        ),
        coverage_claim_time,
        coverage: vec![
            file_store_oracles::coverage::RadioHexSignalLevel {
                location: "8a1fb46622dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store_oracles::coverage::RadioHexSignalLevel {
                location: "8a1fb46632dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store_oracles::coverage::RadioHexSignalLevel {
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
    let bkey: PublicKeyBinary = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
        .parse()
        .unwrap();
    let key = KeyType::from(&bkey);

    assert!(cache.fetch_coverage_object(&uuid, key).await?.is_none());

    let co1 = file_store_oracles::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store_oracles::coverage::KeyType::HotspotKey(bkey.clone()),
        coverage_claim_time,
        coverage: vec![file_store_oracles::coverage::RadioHexSignalLevel {
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

    let co2 = file_store_oracles::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store_oracles::coverage::KeyType::HotspotKey(bkey),
        coverage_claim_time,
        coverage: vec![file_store_oracles::coverage::RadioHexSignalLevel {
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
impl IsAuthorized for AllPubKeysAuthed {
    async fn is_authorized(
        &self,
        _pub_key: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}

fn wifi_heartbeats(
    num: usize,
    start: DateTime<Utc>,
    hotspot_key: PublicKeyBinary,
    lat: f64,
    lon: f64,
    coverage_object: Uuid,
) -> impl Iterator<Item = WifiHeartbeatIngestReport> {
    (0..num).map(move |i| {
        let report = WifiHeartbeat {
            pubkey: hotspot_key.clone(),
            lat,
            lon,
            operation_mode: true,
            location_validation_timestamp: Some(start + Duration::hours(i as i64)),
            coverage_object: Vec::from(coverage_object.into_bytes()),
            timestamp: DateTime::<Utc>::MIN_UTC,
            location_source: LocationSource::Asserted,
        };
        WifiHeartbeatIngestReport {
            report,
            received_timestamp: start + Duration::hours(i as i64),
        }
    })
}

fn bytes_per_s(mbps: u64) -> u64 {
    mbps * 125000
}

fn good_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
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

async fn process_wifi_input(
    pool: &PgPool,
    epoch: &Range<DateTime<Utc>>,
    coverage_objs: impl Iterator<Item = CoverageObjectIngestReport>,
    heartbeats: impl Iterator<Item = WifiHeartbeatIngestReport>,
) -> anyhow::Result<()> {
    let coverage_objects = CoverageObjectCache::new(pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();
    let location_cache = LocationCache::new(pool);

    let mut transaction = pool.begin().await?;
    let mut coverage_objs = pin!(CoverageObject::validate_coverage_objects(
        &AllPubKeysAuthed,
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
        &GatewayClientAllOwnersValid,
        &coverage_objects,
        &location_cache,
        2000,
        epoch,
        &MockGeofence,
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
    let pub_key = PublicKeyBinary::from(vec![1]);

    let coverage_object = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: pub_key.clone(),
            uuid,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(pub_key.clone()),
            coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 120
                signal_level("8c2681a306635ff", SignalLevel::Medium)?, // 60
                signal_level("8c2681a3065d3ff", SignalLevel::Low)?,  // 0
            ],
            trust_score: 1000,
        },
    };

    let heartbeats = wifi_heartbeats(
        13,
        start,
        pub_key.clone(),
        40.019427814,
        -105.27158489,
        uuid,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object].into_iter(),
        heartbeats,
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        good_speedtest(pub_key.clone(), last_timestamp),
        good_speedtest(pub_key.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(pub_key.clone(), SpeedtestAverage::from(owner_speedtests));
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
        coverage_shares.test_hotspot_reward_shares(&pub_key),
        dec!(180)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_two(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let hs_pubkey_1 = PublicKeyBinary::from(vec![1]);
    let hs_pubkey_2 = PublicKeyBinary::from(vec![2]);

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pubkey_1.clone()),
            // older then in co2
            coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 120
                signal_level("8c2681a306635ff", SignalLevel::Medium)?, // 60
                signal_level("8c2681a3066e7ff", SignalLevel::Low)?,  // 0
            ],
            trust_score: 1000,
        },
    };
    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pubkey_2.clone()),
            coverage_claim_time: "2022-01-31 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3065adff", SignalLevel::High)?, // 120
                signal_level("8c2681a306635ff", SignalLevel::Medium)?, // 60 * 0.5 = 30 (this hex is
                // shared
                signal_level("8c2681a3065d7ff", SignalLevel::Low)?, // 0
            ],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        hs_pubkey_1.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );

    let heartbeats_2 = wifi_heartbeats(
        13,
        start,
        hs_pubkey_2.clone(),
        40.019427814,
        -105.27158489,
        uuid_2,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        degraded_speedtest(hs_pubkey_1.clone(), last_timestamp),
        degraded_speedtest(hs_pubkey_1.clone(), end),
    ];

    let speedtests_2 = vec![
        good_speedtest(hs_pubkey_2.clone(), last_timestamp),
        good_speedtest(hs_pubkey_2.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(hs_pubkey_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(hs_pubkey_2.clone(), SpeedtestAverage::from(speedtests_2));
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
        coverage_shares.test_hotspot_reward_shares(&hs_pubkey_1),
        (dec!(120) + dec!(60) + dec!(0)) * dec!(0.5) // speedtest degraded
    );

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pubkey_2),
        (dec!(120) + dec!(60) * dec!(0.5) + dec!(0))
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_three(pool: PgPool) -> anyhow::Result<()> {
    // Scenario: Hex overlapping + hex boosting
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();
    let uuid_4 = Uuid::new_v4();
    let uuid_5 = Uuid::new_v4();

    // All coverage objects share the same hexes
    let hs_pub_key_1 = PublicKeyBinary::from(vec![1]);
    let hs_pub_key_2 = PublicKeyBinary::from(vec![2]);
    let hs_pub_key_3 = PublicKeyBinary::from(vec![3]);
    let hs_pub_key_4 = PublicKeyBinary::from(vec![4]);
    let hs_pub_key_5 = PublicKeyBinary::from(vec![5]);

    let expected_base_cp_co1 = dec!(240);
    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pub_key_1.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 120
                signal_level("8c2681a3065d3ff", SignalLevel::Medium)?, // 60 * 2 = 120
                signal_level("8c2681a306635ff", SignalLevel::Low)?,  // 0 * 3 = 0
                                                                     // = 240 (but rank-limited to top 3 hexes for outdoor)
            ],
            trust_score: 1000,
        },
    };

    let expected_base_cp_co2 = dec!(120);
    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pub_key_2.clone()),
            coverage_claim_time: "2022-02-02 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 120 * 0.5 = 60
                signal_level("8c2681a3065d3ff", SignalLevel::Medium)?, // 60 * 0.5 * 2 = 60
                signal_level("8c2681a306635ff", SignalLevel::Low)?,  // 0 * 0.5 * 3 = 0
                                                                     // = 120
            ],
            trust_score: 1000,
        },
    };

    let expected_base_cp_co3 = dec!(60);
    let coverage_object_3 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_3,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pub_key_3.clone()),
            coverage_claim_time: "2022-02-03 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 120 * 0.25  = 30
                signal_level("8c2681a3065d3ff", SignalLevel::Medium)?, // 60 * 0.25 * 2 = 30
                signal_level("8c2681a306635ff", SignalLevel::Low)?,  // 0 * 0.25 * 3 = 0
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_4 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_4,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pub_key_4.clone()),
            coverage_claim_time: "2022-02-04 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?, // 16
                signal_level("8c2681a3065d3ff", SignalLevel::Medium)?, // 8 * 2 = 16
                signal_level("8c2681a306635ff", SignalLevel::Low)?,  // 4 * 3 = 12
                                                                     // = 48
            ],
            trust_score: 1000,
        },
    };

    let coverage_object_5 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: PublicKeyBinary::from(vec![1]),
            uuid: uuid_5,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(hs_pub_key_5.clone()),
            coverage_claim_time: "2022-02-05 00:00:00.000000000 UTC".parse()?,
            indoor: false,
            signature: Vec::new(),
            coverage: vec![
                signal_level("8c2681a3064d9ff", SignalLevel::High)?,
                signal_level("8c2681a3065d3ff", SignalLevel::Medium)?,
                signal_level("8c2681a306635ff", SignalLevel::Low)?,
            ],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        hs_pub_key_1.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );
    let heartbeats_2 = wifi_heartbeats(
        13,
        start,
        hs_pub_key_2.clone(),
        40.019427814,
        -105.27158489,
        uuid_2,
    );
    let heartbeats_3 = wifi_heartbeats(
        13,
        start,
        hs_pub_key_3.clone(),
        40.019427814,
        -105.27158489,
        uuid_3,
    );
    let heartbeats_4 = wifi_heartbeats(
        13,
        start,
        hs_pub_key_4.clone(),
        40.019427814,
        -105.27158489,
        uuid_4,
    );
    let heartbeats_5 = wifi_heartbeats(
        13,
        start,
        hs_pub_key_5.clone(),
        40.019427814,
        -105.27158489,
        uuid_5,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![
            coverage_object_1,
            coverage_object_2,
            coverage_object_3,
            coverage_object_4,
            coverage_object_5,
        ]
        .into_iter(),
        heartbeats_1
            .chain(heartbeats_2)
            .chain(heartbeats_3)
            .chain(heartbeats_4)
            .chain(heartbeats_5),
    )
    .await
    .unwrap();

    let last_timestamp = end - Duration::hours(12);
    let expected_speedtest_mult_co1 = dec!(0.25);
    let speedtests_1 = vec![
        poor_speedtest(hs_pub_key_1.clone(), last_timestamp),
        poor_speedtest(hs_pub_key_1.clone(), end),
    ];
    let expected_speedtest_mult_co2 = dec!(0.25);
    let speedtests_2 = vec![
        poor_speedtest(hs_pub_key_2.clone(), last_timestamp),
        poor_speedtest(hs_pub_key_2.clone(), end),
    ];
    let expected_speedtest_mult_co3 = dec!(1);
    let speedtests_3 = vec![
        good_speedtest(hs_pub_key_3.clone(), last_timestamp),
        good_speedtest(hs_pub_key_3.clone(), end),
    ];
    let speedtests_4 = vec![
        good_speedtest(hs_pub_key_4.clone(), last_timestamp),
        good_speedtest(hs_pub_key_4.clone(), end),
    ];
    let speedtests_5 = vec![
        good_speedtest(hs_pub_key_5.clone(), last_timestamp),
        good_speedtest(hs_pub_key_5.clone(), end),
    ];

    let mut averages = HashMap::new();
    averages.insert(hs_pub_key_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(hs_pub_key_2.clone(), SpeedtestAverage::from(speedtests_2));
    averages.insert(hs_pub_key_3.clone(), SpeedtestAverage::from(speedtests_3));
    averages.insert(hs_pub_key_4.clone(), SpeedtestAverage::from(speedtests_4));
    averages.insert(hs_pub_key_5.clone(), SpeedtestAverage::from(speedtests_5));
    let speedtest_avgs = SpeedtestAverages { averages };

    let mut boosted_hexes = BoostedHexes::default();
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8c2681a3064d9ff)?,
        BoostedHexInfo {
            location: Cell::from_raw(0x8c2681a3064d9ff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(1).unwrap()],
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8c2681a3065d3ff)?,
        BoostedHexInfo {
            location: Cell::from_raw(0x8c2681a3065d3ff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(2).unwrap()],
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );
    boosted_hexes.hexes.insert(
        Cell::from_raw(0x8c2681a306635ff)?,
        BoostedHexInfo {
            location: Cell::from_raw(0x8c2681a306635ff)?,
            start_ts: None,
            end_ts: None,
            period_length: Duration::hours(1),
            multipliers: vec![NonZeroU32::new(3).unwrap()],
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    );

    // Make all hotspots eligible to get boosted rewards
    let mut unique_connections = UniqueConnectionCounts::default();
    let hotspots = vec![
        hs_pub_key_1.clone(),
        hs_pub_key_2.clone(),
        hs_pub_key_3.clone(),
        hs_pub_key_4.clone(),
        hs_pub_key_5.clone(),
    ];

    for hotspot in hotspots {
        unique_connections.insert(hotspot, 30);
    }

    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &boosted_hexes,
        &BoostedHexEligibility::new(unique_connections.clone()),
        &BannedRadios::default(),
        &unique_connections,
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pub_key_1),
        expected_base_cp_co1 * expected_speedtest_mult_co1
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pub_key_2),
        expected_base_cp_co2 * expected_speedtest_mult_co2
    );

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pub_key_3),
        expected_base_cp_co3 * expected_speedtest_mult_co3
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pub_key_4),
        dec!(0)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&hs_pub_key_5),
        dec!(0)
    );

    Ok(())
}

#[sqlx::test]
async fn scenario_four(pool: PgPool) -> anyhow::Result<()> {
    // Check overlapping wifi indoor
    // https://github.com/helium/HIP/blob/51ccfb5a4dcb82895d5f44ed812bcc77273bd270/0105-modification-of-mobile-subdao-hex-limits.md#indoor-wi-fi
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let pub_key_1 = PublicKeyBinary::from(vec![1]);
    let pub_key_2 = PublicKeyBinary::from(vec![2]);

    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: pub_key_1.clone(),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(pub_key_1.clone()),
            coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_power("8c2681a3064d9ff", SignalLevel::High, -9400)?],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: pub_key_2.clone(),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(pub_key_2.clone()),
            coverage_claim_time: "2022-01-02 00:00:00.000000000 UTC".parse()?,
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_power("8c2681a3064d9ff", SignalLevel::High, -9400)?],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        pub_key_1.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );
    let heartbeats_2 = wifi_heartbeats(
        13,
        start,
        pub_key_2.clone(),
        40.019427814,
        -105.27158489,
        uuid_2,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await
    .unwrap();

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        good_speedtest(pub_key_1.clone(), last_timestamp),
        good_speedtest(pub_key_1.clone(), end),
    ];
    let speedtests_2 = vec![
        good_speedtest(pub_key_2.clone(), last_timestamp),
        good_speedtest(pub_key_2.clone(), end),
    ];

    let mut averages = HashMap::new();
    averages.insert(pub_key_1.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(pub_key_2.clone(), SpeedtestAverage::from(speedtests_2));
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
        coverage_shares.test_hotspot_reward_shares(&pub_key_1),
        dec!(400)
    );

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&pub_key_2),
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

    let coverage_object = file_store_oracles::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid: coverage_object_uuid,
        key_type: file_store_oracles::coverage::KeyType::HotspotKey(owner_1.clone()),
        coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
        trust_score: 1000,
    };

    let coverage_object = CoverageObject::validate(coverage_object, &AllPubKeysAuthed).await?;

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
            location_validation_timestamp: Some(Utc::now() - Duration::hours(23)),
            operation_mode: true,
            coverage_object: Vec::from(coverage_object_uuid.into_bytes()),
            location_source: LocationSource::Skyhook,
        },
        received_timestamp: Utc::now(),
    };

    let validate = |latlng: LatLng| {
        ValidatedHeartbeat::validate(
            mk_heartbeat(latlng).into(),
            &GatewayClientAllOwnersValid,
            &coverage_object_cache,
            &location_cache,
            max_covered_distance,
            &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
            &MockGeofence,
        )
    };

    let covered_cell_index: CellIndex = "8c2681a3064d9ff".parse()?;
    let covered_latlng = LatLng::from(covered_cell_index);

    // Constrain distances by only moving vertically
    let near_latlng = LatLng::new(40.0194278140, -105.272)?; // 35m
    let med_latlng = LatLng::new(40.0194278140, -105.276)?; // 350m
    let far_latlng = LatLng::new(40.0194278140, -105.3)?; // 2,419m
    let past_latlng = LatLng::new(40.0194278140, 105.2715848904)?; // 10,591,975m

    // It's easy to gloss over floats, let make sure the distances are within the ranges we expect.
    assert!((0.0..=300.0).contains(&covered_latlng.distance_m(near_latlng))); // Indoor low distance <= 300
    assert!((300.0..=400.0).contains(&covered_latlng.distance_m(med_latlng))); // Indoor Medium distance <= 400
    assert!(covered_latlng.distance_m(far_latlng) > 400.0); // Indoor Over Distance => 400
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

#[sqlx::test]
async fn eligible_for_coverage_map_bad_speedtest(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let good_hotspot: PublicKeyBinary =
        "11Bn2erjB83zdCBrE248pTVBpTXSuN8Lur4v4mWFnf5Rpd8XK7n".parse()?;
    // This hotspot will have a failing speedtest and should be removed from coverage map
    let bad_speedtest_hotspot: PublicKeyBinary =
        "11d5KySrfiMgaDoZ7B5CDm3meE1gQhUJ5EHuJvzwiWjdSUGhBsZ".parse()?;

    // All coverage objects share the same hexes
    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: good_hotspot.clone(),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(good_hotspot.clone()),
            coverage_claim_time: "2022-02-01 12:00:00.000000000 UTC".parse()?, // Later
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: bad_speedtest_hotspot.clone(),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(
                bad_speedtest_hotspot.clone(),
            ),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?, // Earlier
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        good_hotspot.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );
    let heartbeats_2 = wifi_heartbeats(
        13,
        start,
        bad_speedtest_hotspot.clone(),
        40.019427814,
        -105.27158489,
        uuid_2,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        poor_speedtest(good_hotspot.clone(), last_timestamp),
        poor_speedtest(good_hotspot.clone(), end),
    ];
    let speedtests_2 = vec![
        failed_speedtest(bad_speedtest_hotspot.clone(), last_timestamp),
        failed_speedtest(bad_speedtest_hotspot.clone(), end),
    ];

    let mut averages = HashMap::new();
    averages.insert(good_hotspot.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(
        bad_speedtest_hotspot.clone(),
        SpeedtestAverage::from(speedtests_2),
    );

    let speedtest_avgs = SpeedtestAverages { averages };

    let boosted_hexes = BoostedHexes::default();
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
        coverage_shares.test_hotspot_reward_shares(&good_hotspot),
        dec!(100)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&bad_speedtest_hotspot),
        dec!(0)
    );
    Ok(())
}

#[sqlx::test]
async fn eligible_for_coverage_map_bad_trust_score(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let good_hotspot: PublicKeyBinary =
        "11Bn2erjB83zdCBrE248pTVBpTXSuN8Lur4v4mWFnf5Rpd8XK7n".parse()?;
    // This hotspot will have a bad trust score and should be removed from coverage map
    let bad_location_hotspot: PublicKeyBinary =
        "11d5KySrfiMgaDoZ7B5CDm3meE1gQhUJ5EHuJvzwiWjdSUGhBsZ".parse()?;

    // All coverage objects share the same hexes
    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: good_hotspot.clone(),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(good_hotspot.clone()),
            coverage_claim_time: "2022-02-01 12:00:00.000000000 UTC".parse()?, // Later
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: bad_location_hotspot.clone(),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(
                bad_location_hotspot.clone(),
            ),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?, // Earlier
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        good_hotspot.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );
    // Making location trust score not valid by making it too far from ^
    let heartbeats_2 = wifi_heartbeats(13, start, bad_location_hotspot.clone(), 0.0, 0.0, uuid_2);

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        poor_speedtest(good_hotspot.clone(), last_timestamp),
        poor_speedtest(good_hotspot.clone(), end),
    ];
    let speedtests_2 = vec![
        poor_speedtest(bad_location_hotspot.clone(), last_timestamp),
        poor_speedtest(bad_location_hotspot.clone(), end),
    ];

    let mut averages = HashMap::new();
    averages.insert(good_hotspot.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(
        bad_location_hotspot.clone(),
        SpeedtestAverage::from(speedtests_2),
    );

    let speedtest_avgs = SpeedtestAverages { averages };

    let boosted_hexes = BoostedHexes::default();
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
        coverage_shares.test_hotspot_reward_shares(&good_hotspot),
        dec!(100)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&bad_location_hotspot),
        dec!(0)
    );
    Ok(())
}

#[sqlx::test]
async fn eligible_for_coverage_map_banned(pool: PgPool) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();

    let good_hotspot: PublicKeyBinary =
        "11Bn2erjB83zdCBrE248pTVBpTXSuN8Lur4v4mWFnf5Rpd8XK7n".parse()?;
    // This hotspot will have a bad trust score and should be removed from coverage map
    let banned_hotspot: PublicKeyBinary =
        "11d5KySrfiMgaDoZ7B5CDm3meE1gQhUJ5EHuJvzwiWjdSUGhBsZ".parse()?;

    // All coverage objects share the same hexes
    let coverage_object_1 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: good_hotspot.clone(),
            uuid: uuid_1,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(good_hotspot.clone()),
            coverage_claim_time: "2022-02-01 12:00:00.000000000 UTC".parse()?, // Later
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let coverage_object_2 = CoverageObjectIngestReport {
        received_timestamp: Utc::now(),
        report: file_store_oracles::coverage::CoverageObject {
            pub_key: banned_hotspot.clone(),
            uuid: uuid_2,
            key_type: file_store_oracles::coverage::KeyType::HotspotKey(banned_hotspot.clone()),
            coverage_claim_time: "2022-02-01 00:00:00.000000000 UTC".parse()?, // Earlier
            indoor: true,
            signature: Vec::new(),
            coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
            trust_score: 1000,
        },
    };

    let heartbeats_1 = wifi_heartbeats(
        13,
        start,
        good_hotspot.clone(),
        40.019427814,
        -105.27158489,
        uuid_1,
    );
    let heartbeats_2 = wifi_heartbeats(
        13,
        start,
        banned_hotspot.clone(),
        40.019427814,
        -105.27158489,
        uuid_2,
    );

    process_wifi_input(
        &pool,
        &(start..end),
        vec![coverage_object_1, coverage_object_2].into_iter(),
        heartbeats_1.chain(heartbeats_2),
    )
    .await?;

    let last_timestamp = end - Duration::hours(12);
    let speedtests_1 = vec![
        poor_speedtest(good_hotspot.clone(), last_timestamp),
        poor_speedtest(good_hotspot.clone(), end),
    ];
    let speedtests_2 = vec![
        poor_speedtest(banned_hotspot.clone(), last_timestamp),
        poor_speedtest(banned_hotspot.clone(), end),
    ];

    let mut averages = HashMap::new();
    averages.insert(good_hotspot.clone(), SpeedtestAverage::from(speedtests_1));
    averages.insert(banned_hotspot.clone(), SpeedtestAverage::from(speedtests_2));

    let speedtest_avgs = SpeedtestAverages { averages };

    let boosted_hexes = BoostedHexes::default();
    let reward_period = start..end;
    let heartbeats = HeartbeatReward::validated(&pool, &reward_period);

    // We are banning hotspot2 and therefore should not be in coverage map
    let mut ban_radios = BannedRadios::default();
    ban_radios.insert_sp_banned(banned_hotspot.clone());

    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &boosted_hexes,
        &BoostedHexEligibility::default(),
        &ban_radios,
        &UniqueConnectionCounts::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&good_hotspot),
        dec!(100)
    );
    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&banned_hotspot),
        dec!(0)
    );
    Ok(())
}
