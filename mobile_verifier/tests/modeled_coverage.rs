mod common;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::{CoverageObjectIngestReport, RadioHexSignalLevel},
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
    wifi_heartbeat::{WifiHeartbeat, WifiHeartbeatIngestReport},
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{CoverageObjectValidity, SignalLevel},
};
use mobile_config::boosted_hex_info::{BoostedHexInfo, BoostedHexes};

use mobile_verifier::{
    coverage::{
        set_oracle_boosting_assignments, CoverageClaimTimeCache, CoverageObject,
        CoverageObjectCache, Seniority, UnassignedHex,
    },
    geofence::GeofenceValidator,
    heartbeats::{
        Heartbeat, HeartbeatReward, KeyType, LocationCache, SeniorityUpdate, ValidatedHeartbeat,
    },
    radio_threshold::VerifiedRadioThresholds,
    reward_shares::CoveragePoints,
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    GatewayResolution, GatewayResolver, IsAuthorized,
};
use rust_decimal_macros::dec;
use solana_sdk::pubkey::Pubkey;
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroU32, ops::Range, pin::pin, str::FromStr};
use uuid::Uuid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator<Heartbeat> for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

impl GeofenceValidator<hextree::Cell> for MockGeofence {
    fn in_valid_region(&self, _cell: &hextree::Cell) -> bool {
        true
    }
}

const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
const BOOST_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

#[sqlx::test]
#[ignore]
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
#[ignore]
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
#[ignore]
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
struct AllOwnersValid;

#[async_trait::async_trait]
impl GatewayResolver for AllOwnersValid {
    type Error = std::convert::Infallible;

    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error> {
        Ok(GatewayResolution::AssertedLocation(0x8c2681a3064d9ff))
    }
}

#[derive(Copy, Clone)]
struct AllPubKeysAuthed;

#[async_trait::async_trait]
impl IsAuthorized for AllPubKeysAuthed {
    type Error = std::convert::Infallible;

    async fn is_authorized(
        &self,
        _pub_key: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, Self::Error> {
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
        &AllPubKeysAuthed,
        stream::iter(coverage_objs)
    ));
    while let Some(coverage_obj) = coverage_objs.next().await.transpose()? {
        coverage_obj.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    let unassigned_hexes = UnassignedHex::fetch(pool);
    let _ = set_oracle_boosting_assignments(
        unassigned_hexes,
        &common::MockHexAssignments::default(),
        pool,
    )
    .await?;

    let mut transaction = pool.begin().await?;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        stream::iter(heartbeats.map(Heartbeat::from)),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        2000,
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
            epoch.start,
            latest_seniority,
        );
        seniority_update.execute(&mut transaction).await?;
        heartbeat.save(&mut transaction).await?;
    }
    transaction.commit().await?;

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_one(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-01-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-01-02 00:00:00.000000000 UTC".parse()?;

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
        heartbeats(12, start, &owner, &cbsd_id, 0.0, 0.0, uuid),
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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(coverage_points.hotspot_points(&owner), dec!(1000));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_two(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-02-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-02-02 00:00:00.000000000 UTC".parse()?;

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

    let heartbeats_1 = heartbeats(12, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(12, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);

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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(coverage_points.hotspot_points(&owner_1), dec!(450));
    assert_eq!(coverage_points.hotspot_points(&owner_2), dec!(1000));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_three(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-02-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-02-02 00:00:00.000000000 UTC".parse()?;

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

    let heartbeats_1 = heartbeats(12, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(12, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);
    let heartbeats_3 = heartbeats(12, start, &owner_3, &cbsd_id_3, 0.0, 0.0, uuid_3);
    let heartbeats_4 = heartbeats(12, start, &owner_4, &cbsd_id_4, 0.0, 0.0, uuid_4);
    let heartbeats_5 = heartbeats(12, start, &owner_5, &cbsd_id_5, 0.0, 0.0, uuid_5);
    let heartbeats_6 = heartbeats(12, start, &owner_6, &cbsd_id_6, 0.0, 0.0, uuid_6);

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
        0x8a1fb466d2dffff_u64,
        BoostedHexInfo {
            location: 0x8a1fb466d2dffff_u64,
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
        0x8a1fb49642dffff_u64,
        BoostedHexInfo {
            location: 0x8a1fb49642dffff_u64,
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
        0x8c2681a306607ff_u64,
        BoostedHexInfo {
            // hotspot 1's location
            location: 0x8c2681a306607ff_u64,
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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &boosted_hexes,
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(coverage_points.hotspot_points(&owner_1), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_2), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_3), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_4), dec!(1000));
    assert_eq!(coverage_points.hotspot_points(&owner_5), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_6), dec!(0));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_four(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-01-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-01-02 00:00:00.000000000 UTC".parse()?;

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
        heartbeats(12, start, &owner, &cbsd_id, 0.0, 0.0, uuid),
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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(coverage_points.hotspot_points(&owner), dec!(76));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_five(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-02-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-02-02 00:00:00.000000000 UTC".parse()?;

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

    let heartbeats_1 = heartbeats(12, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(12, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);

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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(
        coverage_points.hotspot_points(&owner_1),
        dec!(76) * dec!(0.5)
    );
    assert_eq!(coverage_points.hotspot_points(&owner_2), dec!(32));

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn scenario_six(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-02-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-02-02 00:00:00.000000000 UTC".parse()?;

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

    let heartbeats_1 = heartbeats(12, start, &owner_1, &cbsd_id_1, 0.0, 0.0, uuid_1);
    let heartbeats_2 = heartbeats(12, start, &owner_2, &cbsd_id_2, 0.0, 0.0, uuid_2);
    let heartbeats_3 = heartbeats(12, start, &owner_3, &cbsd_id_3, 0.0, 0.0, uuid_3);
    let heartbeats_4 = heartbeats(12, start, &owner_4, &cbsd_id_4, 0.0, 0.0, uuid_4);
    let heartbeats_5 = heartbeats(12, start, &owner_5, &cbsd_id_5, 0.0, 0.0, uuid_5);
    let heartbeats_6 = heartbeats(12, start, &owner_6, &cbsd_id_6, 0.0, 0.0, uuid_6);

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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &reward_period,
    )
    .await?;

    assert_eq!(coverage_points.hotspot_points(&owner_1), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_2), dec!(250));
    assert_eq!(coverage_points.hotspot_points(&owner_3), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_4), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_5), dec!(0));
    assert_eq!(coverage_points.hotspot_points(&owner_6), dec!(0));

    Ok(())
}

#[sqlx::test]
#[ignore]
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

    let coverage_object = CoverageObject::validate(coverage_object, &AllPubKeysAuthed).await?;

    let mut transaction = pool.begin().await?;
    coverage_object.save(&mut transaction).await?;
    transaction.commit().await?;

    let hb_1 = WifiHeartbeatIngestReport {
        report: WifiHeartbeat {
            pubkey: owner_1.clone(),
            lon: -105.2715848904,
            lat: 40.0194278140,
            timestamp: DateTime::<Utc>::MIN_UTC,
            location_validation_timestamp: Some(DateTime::<Utc>::MIN_UTC),
            operation_mode: true,
            coverage_object: Vec::from(coverage_object_uuid.into_bytes()),
        },
        received_timestamp: Utc::now(),
    };

    let hb_1: Heartbeat = hb_1.into();

    let hb_2 = WifiHeartbeatIngestReport {
        report: WifiHeartbeat {
            pubkey: owner_1.clone(),
            lon: -105.2344693282443,
            lat: 40.033526907035935,
            timestamp: DateTime::<Utc>::MIN_UTC,
            location_validation_timestamp: Some(DateTime::<Utc>::MIN_UTC),
            operation_mode: true,
            coverage_object: Vec::from(coverage_object_uuid.into_bytes()),
        },
        received_timestamp: Utc::now(),
    };

    let hb_2: Heartbeat = hb_2.into();

    let coverage_object_cache = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let validated_hb_1 = ValidatedHeartbeat::validate(
        hb_1,
        &AllOwnersValid,
        &coverage_object_cache,
        &location_cache,
        2000,
        2000,
        &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_eq!(validated_hb_1.location_trust_score_multiplier, dec!(1.0));

    let validated_hb_2 = ValidatedHeartbeat::validate(
        hb_2.clone(),
        &AllOwnersValid,
        &coverage_object_cache,
        &location_cache,
        1000000,
        2000,
        &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_eq!(validated_hb_2.location_trust_score_multiplier, dec!(0.25));

    let validated_hb_2 = ValidatedHeartbeat::validate(
        hb_2.clone(),
        &AllOwnersValid,
        &coverage_object_cache,
        &location_cache,
        2000,
        1000000,
        &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_eq!(validated_hb_2.location_trust_score_multiplier, dec!(0.25));

    let validated_hb_2 = ValidatedHeartbeat::validate(
        hb_2.clone(),
        &AllOwnersValid,
        &coverage_object_cache,
        &location_cache,
        1000000,
        1000000,
        &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_eq!(validated_hb_2.location_trust_score_multiplier, dec!(1.0));

    Ok(())
}
