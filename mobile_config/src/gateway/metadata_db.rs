use crate::gateway::{
    db::{Gateway, GatewayType},
    service::info::{DeploymentInfo, DeviceType},
};
use chrono::{DateTime, Utc};
use futures::Stream;
use helium_crypto::PublicKeyBinary;
use serde_json;
use sqlx::{Pool, Postgres, Row};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct MobileHotspotInfo {
    entity_key: PublicKeyBinary,
    refreshed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    location: Option<i64>,
    is_full_hotspot: Option<bool>,
    num_location_asserts: Option<i32>,
    is_active: Option<bool>,
    dc_onboarding_fee_paid: Option<i64>,
    device_type: DeviceType,
    deployment_info: Option<DeploymentInfo>,
}

impl MobileHotspotInfo {
    fn compute_hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(
            self.location
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 8])
                .as_ref(),
        );

        hasher.update(
            self.is_full_hotspot
                .map(|l| (l as u32).to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.num_location_asserts
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.is_active
                .map(|l| (l as u32).to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.dc_onboarding_fee_paid
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 8])
                .as_ref(),
        );

        hasher.update(self.device_type.to_string().as_ref());

        hasher.update(
            self.deployment_info
                .as_ref()
                .and_then(|d| d.to_json().ok())
                .unwrap_or_default()
                .as_ref(),
        );

        hasher.finalize().to_string()
    }

    pub fn stream(pool: &Pool<Postgres>) -> impl Stream<Item = Result<Self, sqlx::Error>> + '_ {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT
                    DISTINCT ON (kta.entity_key)
                    kta.entity_key,
                    mhi.refreshed_at,
                    mhi.created_at,
                    mhi.location::bigint,
                    mhi.is_full_hotspot,
                    mhi.num_location_asserts,
                    mhi.is_active,
                    mhi.dc_onboarding_fee_paid::bigint,
                    mhi.device_type::text,
                    mhi.deployment_info::text
                FROM key_to_assets kta
                INNER JOIN mobile_hotspot_infos mhi ON
                    kta.asset = mhi.asset
                WHERE kta.entity_key IS NOT NULL
                    AND mhi.refreshed_at IS NOT NULL
                ORDER BY kta.entity_key, refreshed_at DESC
            "#,
        )
        .fetch(pool)
    }

    pub fn to_gateway(&self) -> anyhow::Result<Option<Gateway>> {
        // We filter out CBRS devices as they are not supported in the Gateway table
        if self.device_type == DeviceType::Cbrs {
            return Ok(None);
        }

        let location = self.location.map(|loc| loc as u64);

        let (antenna, elevation, azimuth) = match self.deployment_info {
            Some(ref info) => match info {
                DeploymentInfo::WifiDeploymentInfo(ref wifi) => {
                    (Some(wifi.antenna), Some(wifi.elevation), Some(wifi.azimuth))
                }
                // Only here to satisfy the match, we return None above if DeviceType::Cbrs
                DeploymentInfo::CbrsDeploymentInfo(_) => (None, None, None),
            },
            None => (None, None, None),
        };

        let refreshed_at = self.refreshed_at.unwrap_or_else(Utc::now);

        Ok(Some(Gateway {
            address: self.entity_key.clone(),
            gateway_type: GatewayType::try_from(self.device_type.clone())?,
            created_at: self.created_at,
            inserted_at: Utc::now(),
            refreshed_at,
            last_changed_at: refreshed_at,
            hash: self.compute_hash(),
            antenna,
            elevation,
            azimuth,
            location,
            // Set to refreshed_at when hotspot has a location, None otherwise
            location_changed_at: if location.is_some() {
                Some(refreshed_at)
            } else {
                None
            },
            location_asserts: self.num_location_asserts.map(|n| n as u32),
            owner: None, // TODO
            owner_changed_at: Some(refreshed_at),
        }))
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for MobileHotspotInfo {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        // device_type came as TEXT from `::text` on jsonb; it may look like `"wifiIndoor"`
        let dt_raw: String = row.try_get("device_type")?;
        let dt_clean = dt_raw.trim_matches('"'); // handle jsonb -> text of a JSON string
        let device_type =
            DeviceType::from_str(dt_clean).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

        let deployment_info: Option<DeploymentInfo> =
            match row.try_get::<Option<String>, _>("deployment_info") {
                Ok(Some(s)) if !s.is_empty() => serde_json::from_str::<DeploymentInfo>(&s).ok(),
                Ok(_) => None,
                Err(_) => None, // be lenient for backward-compat
            };

        Ok(Self {
            entity_key: PublicKeyBinary::from_str(
                &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
            )
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
            refreshed_at: row.get::<Option<DateTime<Utc>>, &str>("refreshed_at"),
            created_at: row.get::<DateTime<Utc>, &str>("created_at"),
            location: row.get::<Option<i64>, &str>("location"),
            is_full_hotspot: row.get::<Option<bool>, &str>("is_full_hotspot"),
            num_location_asserts: row.get::<Option<i32>, &str>("num_location_asserts"),
            is_active: row.get::<Option<bool>, &str>("is_active"),
            dc_onboarding_fee_paid: row.get::<Option<i64>, &str>("dc_onboarding_fee_paid"),
            device_type,
            deployment_info,
        })
    }
}
