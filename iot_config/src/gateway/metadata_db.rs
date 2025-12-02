use chrono::{DateTime, Utc};
use futures::Stream;
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres, Row};
use std::{
    hash::{DefaultHasher, Hasher},
    str::FromStr,
};

use crate::gateway::db::Gateway;

#[derive(Debug, Clone)]
pub struct IOTHotspotInfo {
    entity_key: PublicKeyBinary,
    location: Option<i64>,
    elevation: Option<i32>,
    gain: Option<i32>,
    is_full_hotspot: Option<bool>,
    num_location_asserts: Option<i32>,
    is_active: Option<bool>,
    dc_onboarding_fee_paid: Option<i64>,
    refreshed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl IOTHotspotInfo {
    fn compute_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();

        hasher.write_i64(self.location.unwrap_or(0_i64));

        hasher.write_i32(self.elevation.unwrap_or(0_i32));

        hasher.write_i32(self.gain.unwrap_or(0_i32));

        hasher.write_u8(self.is_full_hotspot.unwrap_or(false) as u8);

        hasher.write_i32(self.num_location_asserts.unwrap_or(0_i32));

        hasher.write_u8(self.is_active.unwrap_or(false) as u8);

        hasher.write_i64(self.dc_onboarding_fee_paid.unwrap_or(0_i64));

        hasher.finish().to_string()
    }

    pub fn stream(pool: &Pool<Postgres>) -> impl Stream<Item = Result<Self, sqlx::Error>> + '_ {
        sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (kta.entity_key)
                kta.entity_key,
                infos.location::bigint,
                infos.elevation::integer,
                infos.gain::integer,
                infos.is_full_hotspot,
                infos.num_location_asserts,
                infos.is_active,
                infos.dc_onboarding_fee_paid::bigint,
                infos.refreshed_at,
                infos.created_at
            FROM iot_hotspot_infos AS infos
            JOIN key_to_assets AS kta ON infos.asset = kta.asset
            ORDER BY kta.entity_key, infos.refreshed_at DESC
            "#,
        )
        .fetch(pool)
    }

    pub fn to_gateway(&self) -> anyhow::Result<Option<Gateway>> {
        let location = self.location.map(|loc| loc as u64);

        Ok(Some(Gateway {
            address: self.entity_key.clone(),
            created_at: self.created_at,
            elevation: self.elevation.map(|e| e as u32),
            gain: self.gain.map(|e| e as u32),
            hash: self.compute_hash(),
            is_active: self.is_active,
            is_full_hotspot: self.is_full_hotspot,
            // Updated via SQL query see Gateway::insert
            last_changed_at: Utc::now(),
            location,
            location_asserts: self.num_location_asserts.map(|n| n as u32),
            // Set to refreshed_at when hotspot has a location, None otherwise
            location_changed_at: if location.is_some() {
                Some(self.refreshed_at.unwrap_or_else(Utc::now))
            } else {
                None
            },
            refreshed_at: self.refreshed_at,
            updated_at: Utc::now(),
        }))
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for IOTHotspotInfo {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        let entity_key_bytes = row.get::<&[u8], &str>("entity_key");
        let entity_key_b58 = bs58::encode(entity_key_bytes).into_string();

        let entity_key = match PublicKeyBinary::from_str(&entity_key_b58) {
            Ok(key) => key,
            Err(err) => {
                // Log the invalid data for investigation
                tracing::warn!(
                    entity_key_bytes = ?hex::encode(entity_key_bytes),
                    entity_key_b58 = %entity_key_b58,
                    error = ?err,
                    "Skipping IOTHotspotInfo with invalid entity_key, failed to decode PublicKeyBinary"
                );
                // Return a decode error which will be filtered out by the stream
                return Err(sqlx::Error::Decode(Box::new(err)));
            }
        };

        Ok(Self {
            entity_key,
            location: row.get::<Option<i64>, &str>("location"),
            elevation: row.get::<Option<i32>, &str>("elevation"),
            gain: row.get::<Option<i32>, &str>("gain"),
            is_full_hotspot: row.get::<Option<bool>, &str>("is_full_hotspot"),
            num_location_asserts: row.get::<Option<i32>, &str>("num_location_asserts"),
            is_active: row.get::<Option<bool>, &str>("is_active"),
            dc_onboarding_fee_paid: row.get::<Option<i64>, &str>("dc_onboarding_fee_paid"),
            refreshed_at: row.get::<Option<DateTime<Utc>>, &str>("refreshed_at"),
            created_at: row.get::<DateTime<Utc>, &str>("created_at"),
        })
    }
}
