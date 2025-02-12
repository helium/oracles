use crate::common::{self, GatewayClientAllOwnersValid};
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::RadioHexSignalLevel,
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
};
use futures::stream::{self, StreamExt};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, OracleBoostingHexAssignment, SignalLevel,
};
use hex_assignments::{Assignment, HexBoostData};
use hextree::Cell;
use mobile_config::boosted_hex_info::BoostedHexes;
use mobile_verifier::{
    coverage::{CoverageClaimTimeCache, CoverageObject, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, Heartbeat, HeartbeatReward, ValidatedHeartbeat},
    reward_shares::CoverageShares,
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    seniority::{Seniority, SeniorityUpdate},
    sp_boosted_rewards_bans::BannedRadios,
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    unique_connections::UniqueConnectionCounts,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet},
    pin::pin,
};
use uuid::Uuid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
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

fn signal_level(hex: &str, signal_level: SignalLevel) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power: 0, // Unused
    })
}

fn hex_cell(loc: &str) -> hextree::Cell {
    hextree::Cell::from_raw(u64::from_str_radix(loc, 16).unwrap()).unwrap()
}

#[sqlx::test]
async fn test_footfall_and_urbanization_report(pool: PgPool) -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();

    fn new_hex_assingment(
        cell: &mut CellIndex,
        footfall: Assignment,
        landtype: Assignment,
        urbanized: Assignment,
        service_provider_override: Assignment,
        assignment_multiplier: u32,
    ) -> OracleBoostingHexAssignment {
        let loc = cell.to_string();
        *cell = cell.succ().unwrap();
        OracleBoostingHexAssignment {
            location: loc,
            assignment_multiplier,
            urbanized: urbanized.into(),
            footfall: footfall.into(),
            landtype: landtype.into(),
            service_provider_override: service_provider_override.into(),
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064d9ff)?;
        use hex_assignments::Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized, no service provider override
            new_hex_assingment(&mut cell, A, A, A, C, 1000),
            new_hex_assingment(&mut cell, A, B, A, C, 1000),
            new_hex_assingment(&mut cell, A, C, A, C, 1000),
            // orange - POI ≥ 1 Not Urbanized, no service provider override
            new_hex_assingment(&mut cell, A, A, B, C, 1000),
            new_hex_assingment(&mut cell, A, B, B, C, 1000),
            new_hex_assingment(&mut cell, A, C, B, C, 1000),
            // light green - Point of Interest Urbanized, no service provider override
            new_hex_assingment(&mut cell, B, A, A, C, 700),
            new_hex_assingment(&mut cell, B, B, A, C, 700),
            new_hex_assingment(&mut cell, B, C, A, C, 700),
            // dark green - Point of Interest Not Urbanized, no service provider override
            new_hex_assingment(&mut cell, B, A, B, C, 500),
            new_hex_assingment(&mut cell, B, B, B, C, 500),
            new_hex_assingment(&mut cell, B, C, B, C, 500),
            // light blue - No POI Urbanized, no service provider override
            new_hex_assingment(&mut cell, C, A, A, C, 400),
            new_hex_assingment(&mut cell, C, B, A, C, 300),
            new_hex_assingment(&mut cell, C, C, A, C, 50),
            // dark blue - No POI Not Urbanized, no service provider override
            new_hex_assingment(&mut cell, C, A, B, C, 200),
            new_hex_assingment(&mut cell, C, B, B, C, 150),
            new_hex_assingment(&mut cell, C, C, B, C, 30),
            // gray - Outside of USA, no service provider override
            new_hex_assingment(&mut cell, A, A, C, C, 0),
            new_hex_assingment(&mut cell, A, B, C, C, 0),
            new_hex_assingment(&mut cell, A, C, C, C, 0),
            new_hex_assingment(&mut cell, B, A, C, C, 0),
            new_hex_assingment(&mut cell, B, B, C, C, 0),
            new_hex_assingment(&mut cell, B, C, C, C, 0),
            new_hex_assingment(&mut cell, C, A, C, C, 0),
            new_hex_assingment(&mut cell, C, B, C, C, 0),
            new_hex_assingment(&mut cell, C, C, C, C, 0),
        ]
    };

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.location, SignalLevel::High).unwrap())
            .collect(),
        trust_score: 1000,
    };

    let mut footfall = HashMap::<hextree::Cell, Assignment>::new();
    let mut urbanized = HashMap::<hextree::Cell, Assignment>::new();
    let mut landtype = HashMap::<hextree::Cell, Assignment>::new();
    let mut service_provider_override = HashSet::<hextree::Cell>::new();

    for hex in hexes.iter() {
        urbanized.insert(hex_cell(&hex.location), hex.urbanized().into());
        footfall.insert(hex_cell(&hex.location), hex.footfall().into());
        landtype.insert(hex_cell(&hex.location), hex.landtype().into());
        if hex.service_provider_override == Assignment::A as i32 {
            service_provider_override.insert(hex_cell(&hex.location));
        }
    }

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let hex_boost_data = HexBoostData::builder()
        .footfall(footfall)
        .landtype(landtype)
        .urbanization(urbanized)
        .service_provider_override(service_provider_override)
        .build()?;
    let oba = common::set_unassigned_oracle_boosting_assignments(&pool, &hex_boost_data).await?;

    assert_eq!(oba.len(), 1);
    assert_eq!(oba[0].assignments, hexes);

    Ok(())
}

#[sqlx::test]
async fn test_footfall_and_urbanization_and_landtype_and_service_provider_override(
    pool: PgPool,
) -> anyhow::Result<()> {
    let end: DateTime<Utc> = Utc::now() + Duration::minutes(10);
    let start: DateTime<Utc> = end - Duration::days(1);

    struct TestHex {
        loc: String,
        landtype: Assignment,
        footfall: Assignment,
        urbanized: Assignment,
        service_provider_override: Assignment,
        expected_score: Decimal,
    }

    impl TestHex {
        fn new(
            cell: &mut CellIndex,
            footfall: Assignment,
            landtype: Assignment,
            urbanized: Assignment,
            service_provider_override: Assignment,
            expected_score: usize,
        ) -> Self {
            let loc = cell.to_string();
            *cell = cell.succ().unwrap();
            Self {
                loc,
                landtype,
                footfall,
                urbanized,
                service_provider_override,
                expected_score: Decimal::from(expected_score),
            }
        }
    }

    let hexes = {
        // NOTE(mj): Cell is mutated in constructor to keep elements aligned for readability
        let mut cell = CellIndex::try_from(0x8c2681a3064d9ff)?;
        use hex_assignments::Assignment::*;
        vec![
            // yellow - POI ≥ 1 Urbanized, no service provider override
            TestHex::new(&mut cell, A, A, A, C, 400),
            TestHex::new(&mut cell, A, B, A, C, 400),
            TestHex::new(&mut cell, A, C, A, C, 400),
            // orange - POI ≥ 1 Not Urbanized, no service provider override
            TestHex::new(&mut cell, A, A, B, C, 400),
            TestHex::new(&mut cell, A, B, B, C, 400),
            TestHex::new(&mut cell, A, C, B, C, 400),
            // light green - Point of Interest Urbanized, no service provider override
            TestHex::new(&mut cell, B, A, A, C, 280),
            TestHex::new(&mut cell, B, B, A, C, 280),
            TestHex::new(&mut cell, B, C, A, C, 280),
            // dark green - Point of Interest Not Urbanized, no service provider override
            TestHex::new(&mut cell, B, A, B, C, 200),
            TestHex::new(&mut cell, B, B, B, C, 200),
            TestHex::new(&mut cell, B, C, B, C, 200),
            // light blue - No POI Urbanized, no service provider override
            TestHex::new(&mut cell, C, A, A, C, 160),
            TestHex::new(&mut cell, C, B, A, C, 120),
            TestHex::new(&mut cell, C, C, A, C, 20),
            // dark blue - No POI Not Urbanized, no service provider override
            TestHex::new(&mut cell, C, A, B, C, 80),
            TestHex::new(&mut cell, C, B, B, C, 60),
            TestHex::new(&mut cell, C, C, B, C, 12),
            // gray - Outside of USA, no service provider override
            TestHex::new(&mut cell, A, A, C, C, 0),
            TestHex::new(&mut cell, A, B, C, C, 0),
            TestHex::new(&mut cell, A, C, C, C, 0),
            TestHex::new(&mut cell, B, A, C, C, 0),
            TestHex::new(&mut cell, B, B, C, C, 0),
            TestHex::new(&mut cell, B, C, C, C, 0),
            TestHex::new(&mut cell, C, A, C, C, 0),
            TestHex::new(&mut cell, C, B, C, C, 0),
            TestHex::new(&mut cell, C, C, C, C, 0),
            // gray - Outside of USA, HAS service provider override
            TestHex::new(&mut cell, C, C, C, A, 100),
        ]
    };
    let sum = hexes.iter().map(|h| h.expected_score).sum::<Decimal>();

    assert_eq!(28, hexes.len());
    assert_eq!(dec!(4392), sum);

    let mut footfall = HashMap::new();
    let mut landtype = HashMap::new();
    let mut urbanized = HashMap::new();
    let mut service_provider_override = HashSet::<Cell>::new();

    for hex in hexes.iter() {
        footfall.insert(hex_cell(&hex.loc), hex.footfall);
        urbanized.insert(hex_cell(&hex.loc), hex.urbanized);
        landtype.insert(hex_cell(&hex.loc), hex.landtype);
        if hex.service_provider_override == Assignment::A {
            service_provider_override.insert(hex_cell(&hex.loc));
        }
    }

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: start,
        indoor: true,
        signature: Vec::new(),
        coverage: hexes
            .iter()
            .map(|hex| signal_level(&hex.loc, SignalLevel::High).unwrap())
            .collect(),
        trust_score: 1000,
    };

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let hex_boost_data = HexBoostData::builder()
        .footfall(footfall)
        .landtype(landtype)
        .urbanization(urbanized)
        .service_provider_override(service_provider_override)
        .build()?;
    let _ = common::set_unassigned_oracle_boosting_assignments(&pool, &hex_boost_data).await?;

    let heartbeat_owner = owner.clone();
    let heartbeats = heartbeats(13, start, &heartbeat_owner, &cbsd_id, 0.0, 0.0, uuid);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();
    let location_cache = LocationCache::new(&pool);

    let epoch = start..end;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        stream::iter(heartbeats.map(Heartbeat::from)),
        &GatewayClientAllOwnersValid,
        &coverage_objects,
        &location_cache,
        2000,
        &epoch,
        &MockGeofence,
    ));
    let mut transaction = pool.begin().await?;
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

    let last_timestamp = end - Duration::hours(12);
    let owner_speedtests = vec![
        acceptable_speedtest(owner.clone(), last_timestamp),
        acceptable_speedtest(owner.clone(), end),
    ];
    let mut averages = HashMap::new();
    averages.insert(owner.clone(), SpeedtestAverage::from(owner_speedtests));
    let speedtest_avgs = SpeedtestAverages { averages };

    let heartbeats = HeartbeatReward::validated(&pool, &epoch);
    let coverage_shares = CoverageShares::new(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &BoostedHexEligibility::default(),
        &BannedRadios::default(),
        &UniqueConnectionCounts::default(),
        &epoch,
    )
    .await
    .context("aggregating points")?;

    // (Footfall, Landtype, Urbanized, Service Provider Selected)
    // Hex   | Assignment | Points Equation | Sum
    // -----------------------------------------------
    // == yellow - POI ≥ 1 Urbanized, no service provider override
    // hex1  | A, A, A, C    | 100 * 1         | 100
    // hex2  | A, B, A, C    | 100 * 1         | 100
    // hex3  | A, C, A, C    | 100 * 1         | 100
    // == orange - POI ≥ 1 Not Urbanized, no service provider override
    // hex4  | A, A, B, C    | 100 * 1         | 100
    // hex5  | A, B, B, C    | 100 * 1         | 100
    // hex6  | A, C, B, C    | 100 * 1         | 100
    // == light green - Point of Interest Urbanized, no service provider override
    // hex7  | B, A, A, C    | 100 * 0.70      | 70
    // hex8  | B, B, A, C    | 100 * 0.70      | 70
    // hex9  | B, C, A, C    | 100 * 0.70      | 70
    // == dark green - Point of Interest Not Urbanized, no service provider override
    // hex10 | B, A, B, C    | 100 * 0.50      | 50
    // hex11 | B, B, B, C    | 100 * 0.50      | 50
    // hex12 | B, C, B, C    | 100 * 0.50      | 50
    // == light blue - No POI Urbanized, no service provider override
    // hex13 | C, A, A, C    | 100 * 0.40      | 40
    // hex14 | C, B, A, C    | 100 * 0.30      | 30
    // hex15 | C, C, A, C    | 100 * 0.05      | 5
    // == dark blue - No POI Not Urbanized, no service provider override
    // hex16 | C, A, B, C    | 100 * 0.20      | 20
    // hex17 | C, B, B, C    | 100 * 0.15      | 15
    // hex18 | C, C, B, C    | 100 * 0.03      | 3
    // == gray - Outside of USA, no service provider override
    // hex19 | A, A, C, C    | 100 * 0.00     | 0
    // hex20 | A, B, C, C    | 100 * 0.00     | 0
    // hex21 | A, C, C, C    | 100 * 0.00     | 0
    // hex22 | B, A, C, C    | 100 * 0.00     | 0
    // hex23 | B, B, C, C    | 100 * 0.00     | 0
    // hex24 | B, C, C, C    | 100 * 0.00     | 0
    // hex25 | C, A, C, C    | 100 * 0.00     | 0
    // hex26 | C, B, C, C    | 100 * 0.00     | 0
    // hex27 | C, C, C, C    | 100 * 0.00     | 0
    // == gray - Outside of USA, HAS service provider override
    // hex28 | A, A, C, A    | 100 * 1.00     | 100

    // -----------------------------------------------
    //                                     = 1,173

    assert_eq!(
        coverage_shares.test_hotspot_reward_shares(&(owner, Some(cbsd_id.clone()))),
        dec!(1173.0)
    );

    Ok(())
}
