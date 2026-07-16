use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::LocationSource;
use mobile_verifier::{
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, Heartbeat, ValidatedHeartbeat},
};
use rust_decimal_macros::dec;
use sqlx::PgPool;

use crate::common::GatewayClientAllOwnersValid;

const PUB_KEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

#[sqlx::test]
async fn heartbeat_uses_last_good_location_when_invalid_location(
    pool: PgPool,
) -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let location_cache = LocationCache::new(&pool);

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

#[sqlx::test]
async fn heartbeat_will_use_last_good_location_from_db(pool: PgPool) -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let location_cache = LocationCache::new(&pool);

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

    location_cache.delete_last_location(&hotspot).await;
    let mut transaction = pool.begin().await?;
    validated_heartbeat_1.clone().save(&mut transaction).await?;
    transaction.commit().await?;

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

#[sqlx::test]
async fn heartbeat_does_not_use_last_good_location_when_more_than_24_hours(
    pool: PgPool,
) -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let location_cache = LocationCache::new(&pool);

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
