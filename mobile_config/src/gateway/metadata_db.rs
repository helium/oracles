use crate::gateway::{
    db::{Gateway, GatewayType},
    service::info::{DeploymentInfo, DeviceType},
};
use chrono::{DateTime, Utc};
use futures::Stream;
use helium_crypto::PublicKeyBinary;
use sqlx::{types::Json, Pool, Postgres, Row};
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
    fn hash(&self) -> String {
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
                    mhi.asset,
                    mhi.refreshed_at,
                    mhi.created_at,
                    mhi.location::bigint,
                    mhi.is_full_hotspot::int,
                    mhi.num_location_asserts,
                    mhi.is_active::int,
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

        Ok(Some(Gateway {
            address: self.entity_key.clone(),
            gateway_type: GatewayType::try_from(self.device_type.clone())?,
            created_at: self.created_at,
            updated_at: Utc::now(),
            refreshed_at: self.refreshed_at.unwrap_or_else(Utc::now),
            // Update via SQL query see Gateway::insert
            last_changed_at: Utc::now(),
            hash: self.hash(),
            antenna,
            elevation,
            azimuth,
            location,
            // Update via SQL query see Gateway::insert
            location_changed_at: None,
            location_asserts: self.num_location_asserts.map(|n| n as u32),
        }))
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for MobileHotspotInfo {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        let device_type = DeviceType::from_str(
            row.get::<Json<String>, &str>("device_type")
                .to_string()
                .as_ref(),
        )
        .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;

        let deployment_info =
            match row.try_get::<Option<Json<DeploymentInfo>>, &str>("deployment_info") {
                Ok(di) => di.map(|v| v.0),
                // We shouldn't fail if an error occurs in this case.
                // This is because the data in this column could be inconsistent,
                // and we don't want to break backward compatibility.
                Err(_e) => None,
            };

        Ok(Self {
            entity_key: PublicKeyBinary::from_str(
                &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
            )
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
            refreshed_at: row.get::<Option<DateTime<Utc>>, &str>("refreshed_at"),
            created_at: row.get::<DateTime<Utc>, &str>("refreshed_at"),
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
