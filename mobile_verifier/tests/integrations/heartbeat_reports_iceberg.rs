//! `hotspots.heartbeat_reports` / `invalid_heartbeat_reports` through the real
//! write path, beside `hotspots.heartbeats` / `invalid_heartbeats`.
//!
//! The point of the report tables is that they hold what the hotspot sent,
//! while the existing tables keep the verifier's decoration. These tests drive
//! heartbeats from ingest reports through validation and
//! `process_validated_heartbeats`, then read both table pairs back.

use chrono::{DateTime, Duration, FixedOffset, SubsecRound, Utc};
use file_store_oracles::wifi_heartbeat::{WifiHeartbeat, WifiHeartbeatIngestReport};
use futures::stream;
use h3o::{CellIndex, LatLng, Resolution};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{self as proto, LocationSource};
use mobile_verifier::{
    geofence::GeofenceValidator,
    heartbeats::{
        last_location::LocationCache, process_validated_heartbeats, Heartbeat, ValidatedHeartbeat,
    },
    iceberg,
};
use uuid::Uuid;

use crate::common::{self, GatewayClientAllOwnersValid};

#[derive(Clone)]
struct AllowAll;

impl GeofenceValidator for AllowAll {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

/// The center of the gateway's asserted cell, as `GatewayClientAllOwnersValid`
/// asserts it.
fn asserted_latlng() -> LatLng {
    let cell: CellIndex = "8c2681a3064d9ff".parse().unwrap();
    cell.into()
}

/// Doubles read back through the Trino client can differ from what was written
/// in the last bit, so coordinates are compared to within 1e-9 degrees
/// (about 0.1 mm).
fn assert_near(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "{actual} is not within 1e-9 of {expected}"
    );
}

fn report(
    hotspot: &PublicKeyBinary,
    received: DateTime<Utc>,
    (lat, lon): (f64, f64),
    location_validation_timestamp: Option<DateTime<Utc>>,
    operation_mode: bool,
) -> WifiHeartbeatIngestReport {
    WifiHeartbeatIngestReport {
        received_timestamp: received,
        report: WifiHeartbeat {
            pubkey: hotspot.clone(),
            lat,
            lon,
            operation_mode,
            location_validation_timestamp,
            coverage_object: Vec::from(Uuid::new_v4().into_bytes()),
            timestamp: received - Duration::seconds(5),
            location_source: LocationSource::Skyhook,
        },
    }
}

async fn process(
    harness: &helium_iceberg::IcebergTestHarness,
    reports: Vec<WifiHeartbeatIngestReport>,
    write_id: &str,
) -> anyhow::Result<()> {
    let writers = common::poc_writers(harness).await?;
    let (sink, _sink_receiver) = common::create_file_sink::<proto::Heartbeat>();
    let cache = LocationCache::new();
    let epoch = DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC;
    let heartbeats = stream::iter(reports.into_iter().map(Heartbeat::from));

    process_validated_heartbeats(
        ValidatedHeartbeat::validate_heartbeats(
            heartbeats,
            &GatewayClientAllOwnersValid,
            &cache,
            &epoch,
            &AllowAll,
        ),
        &sink,
        &writers.heartbeat,
        &writers.heartbeat_report,
        write_id,
    )
    .await
}

#[tokio::test]
async fn reports_keep_what_the_hotspot_sent() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let t0 = Utc::now().trunc_subsecs(0) - Duration::minutes(10);
    let asserted = asserted_latlng();
    let at_asserted = (asserted.lat(), asserted.lng());
    let fix_validated = t0 - Duration::hours(1);
    let stale_validation = t0 - Duration::hours(30);
    let elsewhere = (40.0, -105.0);

    process(
        &harness,
        vec![
            // A: a fresh fix, which becomes the hotspot's cached location.
            report(&hotspot, t0, at_asserted, Some(fix_validated), true),
            // B: no fix, reporting somewhere else. The verifier fills in A's.
            report(&hotspot, t0 + Duration::minutes(5), elsewhere, None, true),
            // C: a fix too old to use. Ingest drops it and the verifier fills
            // in A's.
            report(
                &hotspot,
                t0 + Duration::minutes(6),
                elsewhere,
                Some(stale_validation),
                true,
            ),
            // D: not operational, so rejected.
            report(
                &hotspot,
                t0 + Duration::minutes(7),
                at_asserted,
                None,
                false,
            ),
        ],
        "file-1",
    )
    .await?;

    let trino = harness.trino();
    let mut decorated = iceberg::heartbeat::get_all(trino).await?;
    decorated.sort_by_key(|hb| hb.received_timestamp);
    let mut reports = iceberg::heartbeat_report::get_all(trino).await?;
    reports.sort_by_key(|r| r.received_timestamp);
    let rejected = iceberg::invalid_heartbeat_report::get_all(trino).await?;

    assert_eq!(decorated.len(), 3, "A, B and C are valid");
    assert_eq!(reports.len(), 3, "the same three, as reported");

    // B and C: `heartbeats` holds the cached fix; `heartbeat_reports` holds
    // what the hotspot sent.
    for (decorated, report) in decorated.iter().zip(&reports).skip(1) {
        // heartbeats keeps the carried-forward location.
        assert_near(decorated.lat, at_asserted.0);
        assert_near(decorated.lon, at_asserted.1);
        assert_eq!(
            decorated.location_validation_timestamp,
            Some(fix_validated.into())
        );
        assert_eq!((report.lat, report.lon), elsewhere);
    }
    assert_eq!(reports[1].location_validation_timestamp, None);
    assert_eq!(
        reports[2].location_validation_timestamp,
        Some(stale_validation.into()),
        "the stale validation timestamp is kept, not filtered"
    );

    // The centered point is the reported location's resolution-12 cell center.
    let centered: LatLng = LatLng::new(elsewhere.0, elsewhere.1)?
        .to_cell(Resolution::Twelve)
        .into();
    assert_near(reports[1].centered_lat.unwrap(), centered.lat());
    assert_near(reports[1].centered_lon.unwrap(), centered.lng());

    // D: rejected, with its reason, as reported.
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].reason, "not_operational");
    assert_eq!(
        rejected[0].received_timestamp,
        DateTime::<FixedOffset>::from(t0 + Duration::minutes(7))
    );
    assert_near(rejected[0].lat, at_asserted.0);
    assert_near(rejected[0].lon, at_asserted.1);

    Ok(())
}

#[tokio::test]
async fn reprocessing_a_file_does_not_duplicate_reports() -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let t0 = Utc::now().trunc_subsecs(0) - Duration::minutes(10);
    let asserted = asserted_latlng();
    let reports = || {
        vec![
            report(
                &hotspot,
                t0,
                (asserted.lat(), asserted.lng()),
                Some(t0 - Duration::hours(1)),
                true,
            ),
            report(
                &hotspot,
                t0 + Duration::minutes(1),
                (asserted.lat(), asserted.lng()),
                None,
                false,
            ),
        ]
    };

    process(&harness, reports(), "file-1").await?;
    process(&harness, reports(), "file-1").await?;

    let trino = harness.trino();
    assert_eq!(iceberg::heartbeat_report::get_all(trino).await?.len(), 1);
    assert_eq!(
        iceberg::invalid_heartbeat_report::get_all(trino)
            .await?
            .len(),
        1
    );
    Ok(())
}
