use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use file_store::coverage::RadioHexSignalLevel;
use h3o::LatLng;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_verifier::{
    coverage::{CoverageObject, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::{last_location::LocationCache, HbType, Heartbeat, ValidatedHeartbeat},
    GatewayResolution, GatewayResolver,
};
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const PUB_KEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
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

#[sqlx::test]
async fn heartbeat_uses_last_good_location_when_invalid_location(
    pool: PgPool,
) -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let mut transaction = pool.begin().await?;
    let coverage_object = coverage_object(&hotspot, &mut transaction).await?;
    transaction.commit().await?;

    let validated_heartbeat_1 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .location_validation_timestamp(Utc::now())
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_1.location_trust_score_multiplier,
        dec!(1.0)
    );

    let validated_heartbeat_2 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .latlng((0.0, 0.0))
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
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

    let coverage_objects = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let mut transaction = pool.begin().await?;
    let coverage_object = coverage_object(&hotspot, &mut transaction).await?;
    transaction.commit().await?;

    let validated_heartbeat_1 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .location_validation_timestamp(Utc::now())
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_1.location_trust_score_multiplier,
        dec!(1.0)
    );

    location_cache.delete_last_location(&hotspot).await;
    transaction = pool.begin().await?;
    validated_heartbeat_1.clone().save(&mut transaction).await?;
    transaction.commit().await?;

    let validated_heartbeat_2 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .latlng((0.0, 0.0))
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
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
async fn heartbeat_does_not_use_last_good_location_when_more_than_12_hours(
    pool: PgPool,
) -> anyhow::Result<()> {
    let hotspot = PublicKeyBinary::from_str(PUB_KEY)?;
    let epoch_start = Utc::now() - Duration::days(1);
    let epoch_end = epoch_start + Duration::days(2);

    let coverage_objects = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let mut transaction = pool.begin().await?;
    let coverage_object = coverage_object(&hotspot, &mut transaction).await?;
    transaction.commit().await?;

    let validated_heartbeat_1 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .location_validation_timestamp(Utc::now())
            .timestamp(Utc::now() - Duration::hours(12) - Duration::seconds(1))
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_1.location_trust_score_multiplier,
        dec!(1.0)
    );

    let validated_heartbeat_2 = ValidatedHeartbeat::validate(
        heartbeat(&hotspot, &coverage_object)
            .latlng((0.0, 0.0))
            .build(),
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await?;

    assert_eq!(
        validated_heartbeat_2.location_trust_score_multiplier,
        dec!(0.25)
    );

    Ok(())
}

struct HeartbeatBuilder {
    hotspot: PublicKeyBinary,
    coverage_object: CoverageObject,
    location_validation_timestamp: Option<DateTime<Utc>>,
    latlng: Option<(f64, f64)>,
    timestamp: Option<DateTime<Utc>>,
}

impl HeartbeatBuilder {
    fn new(hotspot: PublicKeyBinary, coverage_object: CoverageObject) -> Self {
        Self {
            hotspot,
            coverage_object,
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
            let lat_lng: LatLng = self
                .coverage_object
                .coverage_object
                .coverage
                .first()
                .unwrap()
                .location
                .into();

            (lat_lng.lat(), lat_lng.lng())
        });

        Heartbeat {
            hb_type: HbType::Wifi,
            hotspot_key: self.hotspot,
            cbsd_id: None,
            operation_mode: true,
            lat,
            lon,
            coverage_object: Some(self.coverage_object.coverage_object.uuid),
            location_validation_timestamp: self.location_validation_timestamp,
            timestamp: self.timestamp.unwrap_or(Utc::now()),
        }
    }
}

fn heartbeat(hotspot: &PublicKeyBinary, coverage_object: &CoverageObject) -> HeartbeatBuilder {
    HeartbeatBuilder::new(hotspot.clone(), coverage_object.clone())
}

async fn coverage_object(
    hotspot: &PublicKeyBinary,
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<CoverageObject> {
    let coverage_object = CoverageObject {
        coverage_object: file_store::coverage::CoverageObject {
            pub_key: hotspot.clone(),
            uuid: Uuid::new_v4(),
            key_type: file_store::coverage::KeyType::HotspotKey(hotspot.clone()),
            coverage_claim_time: Utc::now(),
            coverage: vec![signal_level("8c2681a3064d9ff", proto::SignalLevel::High)?],
            indoor: true,
            trust_score: 0,
            signature: vec![],
        },
        validity: proto::CoverageObjectValidity::Valid,
    };
    coverage_object.save(transaction).await?;

    Ok(coverage_object)
}

fn signal_level(
    hex: &str,
    signal_level: proto::SignalLevel,
) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power: 0, // Unused
    })
}
