use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::LocationSource;
use mobile_verifier::{
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, Heartbeat, ValidatedHeartbeat},
    iceberg::{heartbeat as iceberg_heartbeat, IcebergHeartbeat},
};
use rust_decimal_macros::dec;

use crate::common::GatewayClientAllOwnersValid;

const PUB_KEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

#[tokio::test]
async fn heartbeat_uses_last_good_location_when_invalid_location() -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let location_cache = LocationCache::new();

    let validated_heartbeat_1 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot)
            .location_validation_timestamp(Utc::now())
            .build(),
        &GatewayClientAllOwnersValid,
        &location_cache,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_1.location_trust_score_multiplier,
        dec!(1.0)
    );

    let validated_heartbeat_2 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot).latlng((0.0, 0.0)).build(),
        &GatewayClientAllOwnersValid,
        &location_cache,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    // Despite having no location set, we should still have a 1.0 trust score
    // for the second heartbeat:
    assert_eq!(
        validated_heartbeat_2.location_trust_score_multiplier,
        dec!(1.0)
    );
    assert_eq!(
        validated_heartbeat_1.heartbeat.lat,
        validated_heartbeat_2.heartbeat.lat
    );
    assert_eq!(
        validated_heartbeat_1.heartbeat.lon,
        validated_heartbeat_2.heartbeat.lon
    );

    Ok(())
}

/// A restart loses the in-memory cache. The location a hotspot asserted before
/// the restart is recovered by warming the cache from `poc.heartbeats` in Trino
/// — the replacement for the per-miss `wifi_heartbeats` lookup that used to run
/// against Postgres.
#[tokio::test]
async fn heartbeat_will_use_last_good_location_warmed_from_trino() -> anyhow::Result<()> {
    let harness = crate::common::setup_iceberg().await?;
    let client = trino_client::Client::from_client(harness.owned_trino().await?);

    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    // The location a previous run of the process validated and wrote to iceberg.
    let asserted: LatLng = CellIndex::from_str("8c2681a3064d9ff")?.into();
    let validation_timestamp = Utc::now() - Duration::hours(1);
    let writer = harness
        .get_table_writer_in::<IcebergHeartbeat>(
            iceberg_heartbeat::NAMESPACE,
            iceberg_heartbeat::TABLE_NAME,
        )
        .await?;
    writer
        .write_idempotent(
            "warm-up",
            vec![IcebergHeartbeat {
                hotspot_pubkey: hotspot.to_string(),
                received_timestamp: validation_timestamp.into(),
                heartbeat_timestamp: validation_timestamp.into(),
                device_type: Some("wifi_indoor".to_string()),
                lat: asserted.lat(),
                lon: asserted.lng(),
                coverage_object: String::new(),
                location_validation_timestamp: Some(validation_timestamp.into()),
                distance_to_asserted: Some(0),
                asserted_location: None,
                location_trust_score_multiplier: 1.0,
                location_source: "asserted".to_string(),
            }],
        )
        .await?;

    // A cold cache, warmed the way the daemon warms it at startup.
    let location_cache = LocationCache::from_trino(&client).await?;

    // This heartbeat carries no location validation of its own.
    let validated_heartbeat = ValidatedHeartbeat::validate(
        heartbeat(&hotspot).latlng((0.0, 0.0)).build(),
        &GatewayClientAllOwnersValid,
        &location_cache,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    // It still earns full trust, at the location recovered from Trino.
    assert_eq!(
        validated_heartbeat.location_trust_score_multiplier,
        dec!(1.0)
    );
    assert_eq!(validated_heartbeat.heartbeat.lat, asserted.lat());
    assert_eq!(validated_heartbeat.heartbeat.lon, asserted.lng());

    Ok(())
}

#[tokio::test]
async fn heartbeat_does_not_use_last_good_location_when_more_than_24_hours() -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let location_cache = LocationCache::new();

    let location_validation_timestamp = Utc::now();

    let validated_heartbeat_1 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot)
            .location_validation_timestamp(location_validation_timestamp)
            // within the 24 hour window of validation timestamp
            .timestamp(location_validation_timestamp - Duration::hours(24) + Duration::seconds(1))
            .build(),
        &GatewayClientAllOwnersValid,
        &location_cache,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_1.location_trust_score_multiplier,
        dec!(1.0)
    );

    let validated_heartbeat_2 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot)
            // 24 hours past validation timestamp
            .timestamp(location_validation_timestamp + Duration::hours(24) + Duration::seconds(1))
            .latlng((0.0, 0.0))
            .build(),
        &GatewayClientAllOwnersValid,
        &location_cache,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_2.location_trust_score_multiplier,
        dec!(0.00)
    );

    Ok(())
}

struct HeartbeatBuilder {
    hotspot: PublicKeyBinary,
    location_validation_timestamp: Option<DateTime<Utc>>,
    latlng: Option<(f64, f64)>,
    timestamp: Option<DateTime<Utc>>,
}

impl HeartbeatBuilder {
    fn new(hotspot: PublicKeyBinary) -> Self {
        Self {
            hotspot,
            location_validation_timestamp: None,
            latlng: None,
            timestamp: None,
        }
    }

    fn location_validation_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.location_validation_timestamp = Some(ts);
        self
    }

    fn latlng(mut self, latlng: (f64, f64)) -> Self {
        self.latlng = Some(latlng);
        self
    }

    fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    fn build(self) -> Heartbeat {
        let (lat, lon) = self.latlng.unwrap_or_else(|| {
            // Default to the gateway's asserted location so distance_to_asserted is 0.
            let cell: CellIndex = "8c2681a3064d9ff".parse().unwrap();
            let lat_lng = LatLng::from(cell);
            (lat_lng.lat(), lat_lng.lng())
        });

        Heartbeat {
            hotspot_key: self.hotspot,
            operation_mode: true,
            lat,
            lon,
            coverage_object: None,
            location_validation_timestamp: self.location_validation_timestamp,
            timestamp: self.timestamp.unwrap_or(Utc::now()),
            heartbeat_timestamp: self.timestamp.unwrap_or(Utc::now()),
            location_source: LocationSource::Skyhook,
        }
    }
}

fn heartbeat(hotspot: &PublicKeyBinary) -> HeartbeatBuilder {
    HeartbeatBuilder::new(hotspot.clone())
}
