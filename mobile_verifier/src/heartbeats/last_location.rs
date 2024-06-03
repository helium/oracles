use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use retainer::Cache;
use sqlx::PgPool;

#[derive(sqlx::FromRow, Copy, Clone)]
pub struct LastLocation {
    pub location_validation_timestamp: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
}

impl LastLocation {
    pub fn new(location_validation_timestamp: DateTime<Utc>, lat: f64, lon: f64) -> Self {
        Self {
            location_validation_timestamp,
            lat,
            lon,
        }
    }

    /// Calculates the duration from now in which last_valid_timestamp is 12 hours old
    pub fn duration_to_expiration(&self) -> Duration {
        ((self.location_validation_timestamp + Duration::hours(12)) - Utc::now())
            .max(Duration::zero())
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

    async fn fetch_from_db_and_set(
        &self,
        hotspot: &PublicKeyBinary,
    ) -> anyhow::Result<Option<LastLocation>> {
        let last_location: Option<LastLocation> = sqlx::query_as(
            r#"
            SELECT location_validation_timestamp, lat, lon
            FROM wifi_heartbeats
            WHERE location_validation_timestamp IS NOT NULL
                AND location_validation_timestamp >= $1
                AND hotspot_key = $2
            ORDER BY location_validation_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(Utc::now() - Duration::hours(12))
        .bind(hotspot)
        .fetch_optional(&self.pool)
        .await?;
        self.locations
            .insert(
                hotspot.clone(),
                last_location,
                last_location
                    .map(|x| x.duration_to_expiration())
                    .unwrap_or_else(|| Duration::days(365))
                    .to_std()?,
            )
            .await;
        Ok(last_location)
    }

    pub async fn fetch_last_location(
        &self,
        hotspot: &PublicKeyBinary,
    ) -> anyhow::Result<Option<LastLocation>> {
        Ok(
            if let Some(last_location) = self.locations.get(hotspot).await {
                *last_location
            } else {
                self.fetch_from_db_and_set(hotspot).await?
            },
        )
    }

    pub async fn set_last_location(
        &self,
        hotspot: &PublicKeyBinary,
        last_location: LastLocation,
    ) -> anyhow::Result<()> {
        let duration_to_expiration = last_location.duration_to_expiration();
        self.locations
            .insert(
                hotspot.clone(),
                Some(last_location),
                duration_to_expiration.to_std()?,
            )
            .await;
        Ok(())
    }

    /// Only used for testing.
    pub async fn delete_last_location(&self, hotspot: &PublicKeyBinary) {
        self.locations.remove(hotspot).await;
    }
}
