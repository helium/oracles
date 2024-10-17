use chrono::{DateTime, Duration, Utc};
use file_store::radio_location_estimates::Entity;
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum LocationCacheKey {
    CbrsId(String),
    WifiPubKey(PublicKeyBinary),
}

#[derive(sqlx::FromRow, Copy, Clone, Debug)]
pub struct LocationCacheValue {
    pub lat: f64,
    pub lon: f64,
    pub timestamp: DateTime<Utc>,
}

impl LocationCacheValue {
    pub fn new(lat: f64, lon: f64, timestamp: DateTime<Utc>) -> Self {
        Self {
            lat,
            lon,
            timestamp,
        }
    }
}

/// A cache WiFi/Cbrs heartbeat locations
#[derive(Clone)]
pub struct LocationCache {
    pool: PgPool,
    data: Arc<Mutex<HashMap<LocationCacheKey, LocationCacheValue>>>,
}

impl LocationCache {
    pub fn new(pool: &PgPool) -> Self {
        let data = Arc::new(Mutex::new(
            HashMap::<LocationCacheKey, LocationCacheValue>::new(),
        ));
        let data_clone = data.clone();
        tokio::spawn(async move {
            loop {
                // Sleep 1 hour
                let duration = core::time::Duration::from_secs(60 * 60);
                tokio::time::sleep(duration).await;

                let now = Utc::now();
                // Set the 12-hour threshold
                let twelve_hours_ago = now - Duration::hours(12);

                let mut data = data_clone.lock().await;
                let size_before = data.len() as f64;

                // Retain only values that are within the last 12 hours
                data.retain(|_, v| v.timestamp > twelve_hours_ago);

                let size_after = data.len() as f64;
                info!("cleaned {}", size_before - size_after);
            }
        });
        // TODO: We could spawn an hydrate from DB here?
        Self {
            pool: pool.clone(),
            data,
        }
    }

    pub async fn get(&self, key: LocationCacheKey) -> anyhow::Result<Option<LocationCacheValue>> {
        {
            let data = self.data.lock().await;
            if let Some(&value) = data.get(&key) {
                let now = Utc::now();
                let twelve_hours_ago = now - Duration::hours(12);
                if value.timestamp > twelve_hours_ago {
                    return Ok(None);
                } else {
                    return Ok(Some(value));
                }
            }
        }
        match key {
            LocationCacheKey::WifiPubKey(pub_key_bin) => {
                self.fetch_wifi_and_insert(pub_key_bin).await
            }
            LocationCacheKey::CbrsId(id) => self.fetch_cbrs_and_insert(id).await,
        }
    }

    pub async fn get_all(&self) -> HashMap<LocationCacheKey, LocationCacheValue> {
        let data = self.data.lock().await;
        data.clone()
    }

    pub async fn insert(
        &self,
        key: LocationCacheKey,
        value: LocationCacheValue,
    ) -> anyhow::Result<()> {
        let mut data = self.data.lock().await;
        data.insert(key, value);
        Ok(())
    }

    /// Only used for testing.
    pub async fn remove(&self, key: LocationCacheKey) -> anyhow::Result<()> {
        let mut data = self.data.lock().await;
        data.remove(&key);
        Ok(())
    }

    async fn fetch_wifi_and_insert(
        &self,
        pub_key_bin: PublicKeyBinary,
    ) -> anyhow::Result<Option<LocationCacheValue>> {
        let sqlx_return: Option<LocationCacheValue> = sqlx::query_as(
            r#"
            SELECT lat, lon, location_validation_timestamp AS timestamp
            FROM wifi_heartbeats
            WHERE location_validation_timestamp IS NOT NULL
                AND location_validation_timestamp >= $1
                AND hotspot_key = $2
            ORDER BY location_validation_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(Utc::now() - Duration::hours(12))
        .bind(pub_key_bin.clone())
        .fetch_optional(&self.pool)
        .await?;
        match sqlx_return {
            None => Ok(None),
            Some(value) => {
                let mut data = self.data.lock().await;
                data.insert(LocationCacheKey::WifiPubKey(pub_key_bin), value);
                Ok(Some(value))
            }
        }
    }

    async fn fetch_cbrs_and_insert(
        &self,
        cbsd_id: String,
    ) -> anyhow::Result<Option<LocationCacheValue>> {
        let sqlx_return: Option<LocationCacheValue> = sqlx::query_as(
            r#"
            SELECT lat, lon, location_validation_timestamp AS timestamp
            FROM cbrs_heartbeats
            WHERE location_validation_timestamp IS NOT NULL
                AND location_validation_timestamp >= $1
                AND hotspot_key = $2
            ORDER BY location_validation_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(Utc::now() - Duration::hours(12))
        .bind(cbsd_id.clone())
        .fetch_optional(&self.pool)
        .await?;

        match sqlx_return {
            None => Ok(None),
            Some(value) => {
                let mut data = self.data.lock().await;
                data.insert(LocationCacheKey::CbrsId(cbsd_id), value);
                Ok(Some(value))
            }
        }
    }
}

pub fn key_to_entity(entity: LocationCacheKey) -> Entity {
    match entity {
        LocationCacheKey::CbrsId(id) => Entity::CbrsId(id),
        LocationCacheKey::WifiPubKey(pub_key) => Entity::WifiPubKey(pub_key),
    }
}
