use chrono::{DateTime, Duration, Utc};
use file_store::radio_location_estimates::Entity;
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};

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

type LocationCacheData = HashMap<LocationCacheKey, LocationCacheValue>;

/// A cache WiFi/Cbrs heartbeat locations
#[derive(Clone)]
pub struct LocationCache {
    pool: PgPool,
    wifi: Arc<Mutex<LocationCacheData>>,
    cbrs: Arc<Mutex<LocationCacheData>>,
}

impl LocationCache {
    pub fn new(pool: &PgPool) -> Self {
        let wifi = Arc::new(Mutex::new(HashMap::new()));
        let cbrs = Arc::new(Mutex::new(HashMap::new()));
        // TODO: We could spawn an hydrate from DB here?
        Self {
            pool: pool.clone(),
            wifi,
            cbrs,
        }
    }

    pub async fn get(&self, key: LocationCacheKey) -> anyhow::Result<Option<LocationCacheValue>> {
        {
            let data = self.key_to_lock(&key).await;
            if let Some(&value) = data.get(&key) {
                return Ok(Some(value));
            }
        }
        match key {
            LocationCacheKey::WifiPubKey(pub_key_bin) => {
                self.fetch_wifi_and_insert(pub_key_bin).await
            }
            LocationCacheKey::CbrsId(id) => self.fetch_cbrs_and_insert(id).await,
        }
    }

    pub async fn get_recent(
        &self,
        key: LocationCacheKey,
        when: Duration,
    ) -> anyhow::Result<Option<LocationCacheValue>> {
        {
            let data = self.key_to_lock(&key).await;
            if let Some(&value) = data.get(&key) {
                let now = Utc::now();
                let before = now - when;
                if value.timestamp > before {
                    return Ok(Some(value));
                } else {
                    return Ok(None);
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

    pub async fn get_all(&self) -> LocationCacheData {
        let wifi_data = self.wifi.lock().await;
        let mut wifi_data_cloned = wifi_data.clone();

        let cbrs_data = self.cbrs.lock().await;
        let cbrs_data_cloned = cbrs_data.clone();

        wifi_data_cloned.extend(cbrs_data_cloned);
        wifi_data_cloned
    }

    pub async fn insert(
        &self,
        key: LocationCacheKey,
        value: LocationCacheValue,
    ) -> anyhow::Result<()> {
        let mut data = self.key_to_lock(&key).await;
        data.insert(key, value);
        Ok(())
    }

    /// Only used for testing.
    pub async fn remove(&self, key: LocationCacheKey) -> anyhow::Result<()> {
        let mut data = self.key_to_lock(&key).await;
        data.remove(&key);
        Ok(())
    }

    async fn key_to_lock(&self, key: &LocationCacheKey) -> MutexGuard<'_, LocationCacheData> {
        match key {
            LocationCacheKey::WifiPubKey(_) => self.wifi.lock().await,
            LocationCacheKey::CbrsId(_) => self.cbrs.lock().await,
        }
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
                let key = LocationCacheKey::WifiPubKey(pub_key_bin);
                let mut data = self.key_to_lock(&key).await;
                data.insert(key, value);
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
            SELECT lat, lon, latest_timestamp AS timestamp
            FROM cbrs_heartbeats
            WHERE latest_timestamp IS NOT NULL
                AND latest_timestamp >= $1
                AND hotspot_key = $2
            ORDER BY latest_timestamp DESC
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
                let key = LocationCacheKey::CbrsId(cbsd_id);
                let mut data = self.key_to_lock(&key).await;
                data.insert(key, value);
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
