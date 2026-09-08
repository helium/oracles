use chrono::{DateTime, Duration, Utc};
use file_store_oracles::wifi_heartbeat::{WifiHeartbeat, WifiHeartbeatIngestReport};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::LocationSource;
use mobile_verifier::{
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, Heartbeat, ValidatedHeartbeat},
};
use rust_decimal_macros::dec;
use uuid::Uuid;

use crate::common::GatewayClientAllOwnersValid;

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

/// The reworked `validate` sources radio type from the mobile-config device type
/// (indoor here) and applies the HIP-119 asserted-distance trust curve to the
/// distance between the heartbeat and the gateway's asserted location.
#[tokio::test]
async fn ensure_lower_trust_score_for_distant_heartbeats() -> anyhow::Result<()> {
    let owner: PublicKeyBinary = "11xtYwQYnvkFYnJ9iZ8kmnetYKwhdi87Mcr36e1pVLrhBMPLjV9"
        .parse()
        .unwrap();

    // `GatewayClientAllOwnersValid` asserts this location, so `distance_to_asserted`
    // (and thus the trust multiplier) is measured from it.
    let asserted_latlng = LatLng::from("8c2681a3064d9ff".parse::<CellIndex>()?);

    let location_cache = LocationCache::new();

    let mk_heartbeat = |latlng: LatLng| WifiHeartbeatIngestReport {
        report: WifiHeartbeat {
            pubkey: owner.clone(),
            lon: latlng.lng(),
            lat: latlng.lat(),
            timestamp: DateTime::<Utc>::MIN_UTC,
            location_validation_timestamp: Some(Utc::now() - Duration::hours(23)),
            operation_mode: true,
            coverage_object: Vec::from(Uuid::new_v4().into_bytes()),
            location_source: LocationSource::Skyhook,
        },
        received_timestamp: Utc::now(),
    };

    let validate = |latlng: LatLng| {
        ValidatedHeartbeat::validate(
            mk_heartbeat(latlng).into(),
            &GatewayClientAllOwnersValid,
            &location_cache,
            &(DateTime::<Utc>::MIN_UTC..DateTime::<Utc>::MAX_UTC),
            &MockGeofence,
        )
    };

    // Constrain distances by only moving vertically from the asserted location.
    let near_latlng = LatLng::new(40.0194278140, -105.272)?; // 35m
    let med_latlng = LatLng::new(40.0194278140, -105.276)?; // 350m
    let far_latlng = LatLng::new(40.0194278140, -105.3)?; // 2,419m
    let past_latlng = LatLng::new(40.0194278140, 105.2715848904)?; // ~10,591,975m

    // Sanity-check the distances fall in the expected HIP-119 indoor bands.
    assert!((0.0..=300.0).contains(&asserted_latlng.distance_m(near_latlng)));
    assert!((300.0..=400.0).contains(&asserted_latlng.distance_m(med_latlng)));
    assert!(asserted_latlng.distance_m(far_latlng) > 400.0);
    assert!(asserted_latlng.distance_m(past_latlng) > 400.0);

    assert_eq!(
        validate(near_latlng).await?.location_trust_score_multiplier,
        dec!(1.0)
    );
    assert_eq!(
        validate(med_latlng).await?.location_trust_score_multiplier,
        dec!(0.25)
    );
    assert_eq!(
        validate(far_latlng).await?.location_trust_score_multiplier,
        dec!(0.00)
    );
    assert_eq!(
        validate(past_latlng).await?.location_trust_score_multiplier,
        dec!(0.00)
    );

    Ok(())
}
