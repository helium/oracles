use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::RadioHexSignalLevel,
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectValidity, OracleBoostingHexAssignment, SignalLevel,
};
use mobile_config::boosted_hex_info::BoostedHexes;
use mobile_verifier::{
    boosting_oracles::{Assignment, HexBoostData, UrbanizationData},
    coverage::{
        set_oracle_boosting_assignments, CoverageClaimTimeCache, CoverageObject,
        CoverageObjectCache, Seniority, UnassignedHex,
    },
    geofence::GeofenceValidator,
    heartbeats::{Heartbeat, HeartbeatReward, SeniorityUpdate, ValidatedHeartbeat},
    radio_threshold::VerifiedRadioThresholds,
    reward_shares::CoveragePoints,
    speedtests::Speedtest,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
    GatewayResolution, GatewayResolver,
};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet},
    pin::pin,
};
use uuid::Uuid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator<Heartbeat> for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

impl GeofenceValidator<u64> for MockGeofence {
    fn in_valid_region(&self, _cell: &u64) -> bool {
        true
    }
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

    let hex1 = OracleBoostingHexAssignment {
        location: "8c2681a3064d9ff".to_string(),
        assignment_multiplier: 1000,
        urbanized: Assignment::A.into(),
        footfall: Assignment::A.into(),
    };
    let hex2 = OracleBoostingHexAssignment {
        location: "8c2681a3064d1ff".to_string(),
        assignment_multiplier: 1000,
        urbanized: Assignment::B.into(),
        footfall: Assignment::A.into(),
    };
    let hex3 = OracleBoostingHexAssignment {
        location: "8c450e64dc899ff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::A.into(),
    };
    let hex4 = OracleBoostingHexAssignment {
        location: "8c2681a3064dbff".to_string(),
        assignment_multiplier: 750,
        urbanized: Assignment::A.into(),
        footfall: Assignment::B.into(),
    };
    let hex5 = OracleBoostingHexAssignment {
        location: "8c2681a339365ff".to_string(),
        assignment_multiplier: 500,
        urbanized: Assignment::B.into(),
        footfall: Assignment::B.into(),
    };
    let hex6 = OracleBoostingHexAssignment {
        location: "8c450e64dc89dff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::B.into(),
    };
    let hex7 = OracleBoostingHexAssignment {
        location: "8c2681a3066b3ff".to_string(),
        assignment_multiplier: 400,
        urbanized: Assignment::A.into(),
        footfall: Assignment::C.into(),
    };
    let hex8 = OracleBoostingHexAssignment {
        location: "8c2681a3066b7ff".to_string(),
        assignment_multiplier: 100,
        urbanized: Assignment::B.into(),
        footfall: Assignment::C.into(),
    };
    let hex9 = OracleBoostingHexAssignment {
        location: "8c450e64dc883ff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::C.into(),
    };

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![
            signal_level(&hex1.location, SignalLevel::High)?,
            signal_level(&hex2.location, SignalLevel::High)?,
            signal_level(&hex3.location, SignalLevel::High)?,
            signal_level(&hex4.location, SignalLevel::High)?,
            signal_level(&hex5.location, SignalLevel::High)?,
            signal_level(&hex6.location, SignalLevel::High)?,
            signal_level(&hex7.location, SignalLevel::High)?,
            signal_level(&hex8.location, SignalLevel::High)?,
            signal_level(&hex9.location, SignalLevel::High)?,
        ],
        trust_score: 1000,
    };

    let mut footfall = HashMap::new();
    footfall.insert(hex_cell(&hex1.location), true);
    footfall.insert(hex_cell(&hex2.location), true);
    footfall.insert(hex_cell(&hex3.location), true);
    footfall.insert(hex_cell(&hex4.location), false);
    footfall.insert(hex_cell(&hex5.location), false);
    footfall.insert(hex_cell(&hex6.location), false);

    let mut urbanized = HashSet::new();
    urbanized.insert(hex_cell(&hex1.location));
    urbanized.insert(hex_cell(&hex4.location));
    urbanized.insert(hex_cell(&hex7.location));

    let mut geofence = HashSet::new();
    geofence.insert(hex_cell(&hex1.location));
    geofence.insert(hex_cell(&hex2.location));
    geofence.insert(hex_cell(&hex4.location));
    geofence.insert(hex_cell(&hex5.location));
    geofence.insert(hex_cell(&hex7.location));
    geofence.insert(hex_cell(&hex8.location));

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let unassigned_hexes = UnassignedHex::fetch(&pool);
    let urbanization = UrbanizationData::new(urbanized, geofence);
    let hex_boost_data = HexBoostData::new(urbanization, footfall);
    let oba = set_oracle_boosting_assignments(unassigned_hexes, &hex_boost_data, &pool)
        .await?
        .collect::<Vec<_>>();

    assert_eq!(oba.len(), 1);
    assert_eq!(
        oba[0].assignments,
        vec![hex1, hex2, hex3, hex4, hex5, hex6, hex7, hex8, hex9]
    );

    Ok(())
}

#[sqlx::test]
async fn test_footfall_and_urbanization(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-01-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-01-02 00:00:00.000000000 UTC".parse()?;

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let hex1 = OracleBoostingHexAssignment {
        location: "8c2681a3064d9ff".to_string(),
        assignment_multiplier: 1000,
        urbanized: Assignment::A.into(),
        footfall: Assignment::A.into(),
    };
    let hex2 = OracleBoostingHexAssignment {
        location: "8c2681a3064d1ff".to_string(),
        assignment_multiplier: 1000,
        urbanized: Assignment::B.into(),
        footfall: Assignment::A.into(),
    };
    let hex3 = OracleBoostingHexAssignment {
        location: "8c450e64dc899ff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::A.into(),
    };
    let hex4 = OracleBoostingHexAssignment {
        location: "8c2681a3064dbff".to_string(),
        assignment_multiplier: 750,
        urbanized: Assignment::A.into(),
        footfall: Assignment::B.into(),
    };
    let hex5 = OracleBoostingHexAssignment {
        location: "8c2681a339365ff".to_string(),
        assignment_multiplier: 500,
        urbanized: Assignment::B.into(),
        footfall: Assignment::B.into(),
    };
    let hex6 = OracleBoostingHexAssignment {
        location: "8c450e64dc89dff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::B.into(),
    };
    let hex7 = OracleBoostingHexAssignment {
        location: "8c2681a3066b3ff".to_string(),
        assignment_multiplier: 400,
        urbanized: Assignment::A.into(),
        footfall: Assignment::C.into(),
    };
    let hex8 = OracleBoostingHexAssignment {
        location: "8c2681a3066b7ff".to_string(),
        assignment_multiplier: 100,
        urbanized: Assignment::B.into(),
        footfall: Assignment::C.into(),
    };
    let hex9 = OracleBoostingHexAssignment {
        location: "8c450e64dc883ff".to_string(),
        assignment_multiplier: 0,
        urbanized: Assignment::C.into(),
        footfall: Assignment::C.into(),
    };

    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![
            signal_level(&hex1.location, SignalLevel::High)?,
            signal_level(&hex2.location, SignalLevel::High)?,
            signal_level(&hex3.location, SignalLevel::High)?,
            signal_level(&hex4.location, SignalLevel::High)?,
            signal_level(&hex5.location, SignalLevel::High)?,
            signal_level(&hex6.location, SignalLevel::High)?,
            signal_level(&hex7.location, SignalLevel::High)?,
            signal_level(&hex8.location, SignalLevel::High)?,
            signal_level(&hex9.location, SignalLevel::High)?,
        ],
        trust_score: 1000,
    };

    let mut footfall = HashMap::new();
    footfall.insert(hex_cell(&hex1.location), true);
    footfall.insert(hex_cell(&hex2.location), true);
    footfall.insert(hex_cell(&hex3.location), true);
    footfall.insert(hex_cell(&hex4.location), false);
    footfall.insert(hex_cell(&hex5.location), false);
    footfall.insert(hex_cell(&hex6.location), false);

    let mut urbanized = HashSet::new();
    urbanized.insert(hex_cell(&hex1.location));
    urbanized.insert(hex_cell(&hex4.location));
    urbanized.insert(hex_cell(&hex7.location));

    let mut geofence = HashSet::new();
    geofence.insert(hex_cell(&hex1.location));
    geofence.insert(hex_cell(&hex2.location));
    geofence.insert(hex_cell(&hex4.location));
    geofence.insert(hex_cell(&hex5.location));
    geofence.insert(hex_cell(&hex7.location));
    geofence.insert(hex_cell(&hex8.location));

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let unassigned_hexes = UnassignedHex::fetch(&pool);
    let urbanization = UrbanizationData::new(urbanized, geofence);
    let hex_boost_data = HexBoostData::new(urbanization, footfall);
    let _ = set_oracle_boosting_assignments(unassigned_hexes, &hex_boost_data, &pool).await?;

    let heartbeats = heartbeats(12, start, &owner, &cbsd_id, 0.0, 0.0, uuid);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();

    let epoch = start..end;
    let mut heartbeats = pin!(ValidatedHeartbeat::validate_heartbeats(
        &AllOwnersValid,
        stream::iter(heartbeats.map(Heartbeat::from)),
        &coverage_objects,
        2000,
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
            epoch.start,
            latest_seniority,
        );
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
    let coverage_points = CoveragePoints::aggregate_points(
        &pool,
        heartbeats,
        &speedtest_avgs,
        &BoostedHexes::default(),
        &VerifiedRadioThresholds::default(),
        &epoch,
    )
    .await?;

    // Hex  | Assignment  | Points Equation | Sum
    // --------------------------------------------
    // hex1 | A, A        | 400 * 1    | 400
    // hex2 | A, B        | 400 * 1    | 400
    // hex3 | B, A        | 400 * 0.75 | 300
    // hex4 | B, B        | 400 * 0.50 | 200
    // hex5 | C, A        | 400 * 0.40 | 160
    // hex6 | C, B        | 400 * 0.10 | 40
    // hex7 | A, C        | 400 * 0.00 | 0
    // hex8 | B, C        | 400 * 0.00 | 0
    // hex9 | C, C        | 400 * 0.00 | 0
    // -------------------------------------------
    //                                 = 1,500

    assert_eq!(coverage_points.hotspot_points(&owner), dec!(1500));

    Ok(())
}
