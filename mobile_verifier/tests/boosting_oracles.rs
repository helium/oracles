use chrono::{DateTime, Duration, Utc};
use file_store::{
    coverage::RadioHexSignalLevel,
    heartbeat::{CbrsHeartbeat, CbrsHeartbeatIngestReport},
    speedtest::CellSpeedtest,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CoverageObjectValidity, SignalLevel};
use mobile_config::boosted_hex_info::BoostedHexes;
use mobile_verifier::{
    boosting_oracles::{set_oracle_boosting_assignments, UnassignedHex, Urbanization},
    coverage::{CoverageClaimTimeCache, CoverageObject, CoverageObjectCache, Seniority},
    geofence::GeofenceValidator,
    heartbeats::{Heartbeat, HeartbeatReward, SeniorityUpdate, ValidatedHeartbeat},
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

#[sqlx::test]
async fn test_urbanization(pool: PgPool) -> anyhow::Result<()> {
    let start: DateTime<Utc> = "2022-01-01 00:00:00.000000000 UTC".parse()?;
    let end: DateTime<Utc> = "2022-01-02 00:00:00.000000000 UTC".parse()?;

    let uuid = Uuid::new_v4();
    let cbsd_id = "P27-SCE4255W120200039521XGB0102".to_string();
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9".parse()?;
    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(cbsd_id.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![
            // Urbanized, not boosted
            signal_level("8c2681a3064d9ff", SignalLevel::High)?,
            // Not urbanized, not boosted
            signal_level("8c2681a339a4bff", SignalLevel::High)?,
            // Outside the US, not boosted
            signal_level("8c2681a3066e7ff", SignalLevel::High)?,
        ],
        trust_score: 1000,
    };

    let epoch = start..end;
    // Only the first is urbanized
    let mut urbanized = HashMap::<hextree::Cell, Vec<u8>>::new();
    urbanized.insert(
        hextree::Cell::from_raw(u64::from_str_radix("8c2681a3064d9ff", 16)?)?,
        vec![],
    );

    // The last hex is outside the US
    let mut geofence = HashSet::new();
    geofence.insert(u64::from_str_radix("8c2681a3064d9ff", 16)?);
    geofence.insert(u64::from_str_radix("8c2681a339a4bff", 16)?);

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let urbanization = Urbanization::new_mock(urbanized, geofence);
    let unassigned_hexes = UnassignedHex::fetch_unassigned(&pool);
    let _ = set_oracle_boosting_assignments(unassigned_hexes, &urbanization, &pool).await?;

    let heartbeats = heartbeats(12, start, &owner, &cbsd_id, 0.0, 0.0, uuid);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let coverage_claim_time_cache = CoverageClaimTimeCache::new();

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
        &epoch,
    )
    .await?;

    // Hex  | Points Equation | Sum
    // -----------------------------
    // hex1 | 400             | 400
    // hex3 | 400 * 0.25      | 100
    // hex5 | 400 * 0.00      | 0
    // -----------------------------
    //                        = 500

    assert_eq!(coverage_points.hotspot_points(&owner), dec!(500));

    Ok(())
}
