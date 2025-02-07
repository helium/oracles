use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use retainer::Cache;
use sqlx::PgPool;

use super::Heartbeat;

#[derive(Debug, sqlx::FromRow, Copy, Clone, PartialEq)]
pub struct LastLocation {
    pub location_validation_timestamp: DateTime<Utc>,
    pub latest_timestamp: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
}

impl LastLocation {
    pub fn new(
        location_validation_timestamp: DateTime<Utc>,
        latest_timestamp: DateTime<Utc>,
        lat: f64,
        lon: f64,
    ) -> Self {
        Self {
            location_validation_timestamp,
            latest_timestamp,
            lat,
            lon,
        }
    }

    pub fn from_heartbeat(
        heartbeat: &Heartbeat,
        location_validation_timestamp: DateTime<Utc>,
    ) -> Self {
        Self::new(
            location_validation_timestamp,
            heartbeat.timestamp,
            heartbeat.lat,
            heartbeat.lon,
        )
    }

    fn still_valid(&self, heartbeat_timestamp: DateTime<Utc>) -> bool {
        let diff = heartbeat_timestamp - self.location_validation_timestamp;
        diff <= Duration::hours(24)
    }

    fn cache_expiration_duration(&self) -> Option<std::time::Duration> {
        // A validation_timestamp is valid for 24 hours past itself,
        // but could still be in the past
        let until = self.location_validation_timestamp + Duration::hours(24);
        let diff = until - Utc::now();

        // Converting to_ std() with a negative Duration casts to None
        diff.to_std().ok()
    }
}

/// A cache for previous valid (or invalid) WiFi heartbeat locations
#[derive(Clone)]
pub struct LocationCache {
    pool: PgPool,
    locations: Arc<Cache<PublicKeyBinary, Option<LastLocation>>>,
}

impl LocationCache {
    pub fn new(pool: &PgPool) -> Self {
        let locations = Arc::new(Cache::new());
        let locations_clone = locations.clone();
        tokio::spawn(async move {
            locations_clone
                .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 24))
                .await
        });
        Self {
            pool: pool.clone(),
            locations,
        }
    }

    pub async fn set(&self, hotspot: &PublicKeyBinary, last_location: LastLocation) {
        self.cache_last_location(hotspot, Some(last_location)).await;
    }

    pub async fn get(
        &self,
        hotspot: &PublicKeyBinary,
        heartbeat_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Option<LastLocation>> {
        let location = match self.locations.get(hotspot).await {
            Some(last_location) => {
                // The value may still be cached according to the system clock
                // but not valid based on the time of the heartbeat in question.
                let last = *last_location;
                last.filter(|l| l.still_valid(heartbeat_timestamp))
            }
            None => {
                let last = self.fetch_from_db(hotspot, heartbeat_timestamp).await?;
                self.cache_last_location(hotspot, last).await;
                last
            }
        };

        Ok(location)
    }

    async fn fetch_from_db(
        &self,
        hotspot: &PublicKeyBinary,
        heartbeat_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Option<LastLocation>> {
        let last_location: Option<LastLocation> = sqlx::query_as(
            r#"
            SELECT location_validation_timestamp, latest_timestamp, lat, lon
            FROM wifi_heartbeats
            WHERE location_validation_timestamp IS NOT NULL
                AND latest_timestamp >= $1
                AND hotspot_key = $2
                AND $3 - location_validation_timestamp <= INTERVAL '24 hours'
            ORDER BY latest_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(Utc::now() - Duration::hours(24))
        .bind(hotspot)
        .bind(heartbeat_timestamp)
        .fetch_optional(&self.pool)
        .await?;

        Ok(last_location)
    }

    async fn cache_last_location(
        &self,
        hotspot: &PublicKeyBinary,
        last_location: Option<LastLocation>,
    ) {
        match location_with_expiration(last_location) {
            Some((last, cache_duration)) => {
                self.locations
                    .insert(hotspot.clone(), Some(last), cache_duration)
                    .await;
            }
            None => {
                self.locations
                    .insert(hotspot.clone(), None, Duration::days(365).to_std().unwrap())
                    .await;
            }
        }
    }

    /// Only used for testing.
    pub async fn delete_last_location(&self, hotspot: &PublicKeyBinary) {
        self.locations.remove(hotspot).await;
    }
}

fn location_with_expiration(
    last_location: Option<LastLocation>,
) -> Option<(LastLocation, std::time::Duration)> {
    let last = last_location?;
    let cache_duration = last.cache_expiration_duration()?;
    Some((last, cache_duration))
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Duration, DurationRound, Utc};
    use helium_crypto::PublicKeyBinary;
    use sqlx::PgPool;
    use uuid::Uuid;

    // Make sure test timestamps and DB timestamps have the same granularity
    fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
        ts.duration_trunc(Duration::nanoseconds(10)).unwrap()
    }
    fn hour_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
        ts.duration_trunc(Duration::hours(1)).unwrap()
    }

    async fn insert_heartbeat(
        pool: &PgPool,
        hotspot: &PublicKeyBinary,
        received_timestamp: DateTime<Utc>,
        validation_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO wifi_heartbeats
                (
                    hotspot_key, location_validation_timestamp, latest_timestamp,
                    truncated_timestamp, coverage_object,

                    -- hardcoded values
                    lat, lon, cell_type, distance_to_asserted, location_trust_score_multiplier
                )
            VALUES
                (
                    $1, $2, $3, $4, $5,

                    -- harcoded values
                    0.0, 0.0, 'novagenericwifiindoor', 0, 1000
                )
            "#,
        )
        .bind(hotspot)
        .bind(nanos_trunc(validation_timestamp))
        .bind(nanos_trunc(received_timestamp))
        .bind(hour_trunc(received_timestamp))
        .bind(Uuid::new_v4())
        .execute(pool)
        .await?;

        Ok(())
    }

    fn test_last_location(
        latest_timestamp: DateTime<Utc>,
        location_validation_timestamp: DateTime<Utc>,
    ) -> LastLocation {
        LastLocation {
            location_validation_timestamp: nanos_trunc(location_validation_timestamp),
            latest_timestamp: nanos_trunc(latest_timestamp),
            lat: 0.0,
            lon: 0.0,
        }
    }

    #[sqlx::test]
    async fn test_invalid_validation_timestamp(pool: PgPool) -> anyhow::Result<()> {
        let now = Utc::now();

        let hotspot_one = PublicKeyBinary::from(vec![1]);
        let stale_timestamp = now - Duration::hours(24) - Duration::seconds(1);
        insert_heartbeat(&pool, &hotspot_one, now, stale_timestamp).await?;

        let hotspot_two = PublicKeyBinary::from(vec![2]);
        let limit_timestamp = now - Duration::hours(24);
        insert_heartbeat(&pool, &hotspot_two, now, limit_timestamp).await?;

        let hotspot_three = PublicKeyBinary::from(vec![3]);
        let good_timestamp = now - Duration::hours(12);
        insert_heartbeat(&pool, &hotspot_three, now, good_timestamp).await?;

        let cache = LocationCache::new(&pool);
        assert_eq!(
            None,
            cache.get(&hotspot_one, now).await?,
            "Invalid timestamp current"
        );
        let expect = Some(test_last_location(now, limit_timestamp));
        let cached = cache.get(&hotspot_two, now).await?;
        println!("expect: {:?}", expect);
        println!("cached: {:?}", cached);
        assert_eq!(expect, cached, "Limit timestamp current");
        assert_eq!(
            Some(test_last_location(now, good_timestamp)),
            cache.get(&hotspot_three, now).await?,
            "Good timestamp current"
        );

        // Moving an 1 day into the future should invalidate all timestamps
        // regardless of what has already been cached.
        let future = now + Duration::days(1);
        assert_eq!(
            None,
            cache.get(&hotspot_one, future).await?,
            "Invalid timestamp future"
        );
        assert_eq!(
            None,
            cache.get(&hotspot_two, future).await?,
            "Limit timestamp future"
        );
        assert_eq!(
            None,
            cache.get(&hotspot_three, future).await?,
            "Good timestamp future"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn will_not_cache_invalid_validation_timestamps(pool: PgPool) -> anyhow::Result<()> {
        let cache = LocationCache::new(&pool);

        let now = Utc::now();
        let validation_timestamp = now - Duration::hours(25);

        let hotspot = PublicKeyBinary::from(vec![1]);
        let invalid_location = test_last_location(now, validation_timestamp);
        cache.set(&hotspot, invalid_location).await;

        assert_eq!(None, cache.get(&hotspot, now).await?);

        Ok(())
    }

    #[sqlx::test]
    async fn will_cache_valid_validation_timestamps(pool: PgPool) -> anyhow::Result<()> {
        let cache = LocationCache::new(&pool);

        let now = Utc::now();
        let validation_timestamp = now - Duration::hours(12);

        let hotspot = PublicKeyBinary::from(vec![1]);
        let valid_location = test_last_location(now, validation_timestamp);
        cache.set(&hotspot, valid_location).await;

        assert_eq!(Some(valid_location), cache.get(&hotspot, now).await?);

        Ok(())
    }
}
