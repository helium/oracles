use crate::gateway::{db::GatewayType, service::info::DeploymentInfo};
use chrono::{DateTime, Utc};
use futures::Stream;
use helium_crypto::PublicKeyBinary;
use serde_json;
use sqlx::{Pool, Postgres, Row};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct MobileHotspotInfo {
    pub entity_key: PublicKeyBinary,
    pub refreshed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub location: Option<i64>,
    pub is_full_hotspot: Option<bool>,
    pub num_location_asserts: Option<i32>,
    pub is_active: Option<bool>,
    pub dc_onboarding_fee_paid: Option<i64>,
    pub gateway_type: GatewayType,
    pub deployment_info: Option<DeploymentInfo>,
    pub owner: Option<String>,
}

impl MobileHotspotInfo {
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
                    mhi.deployment_info::text,
                    ao.owner
                FROM key_to_assets kta
                INNER JOIN mobile_hotspot_infos mhi ON
                    kta.asset = mhi.asset
                LEFT JOIN asset_owners ao ON
                    kta.asset = ao.asset
                WHERE kta.entity_key IS NOT NULL
                    AND mhi.refreshed_at IS NOT NULL
                    AND device_type != '"cbrs"'
                ORDER BY kta.entity_key, refreshed_at DESC
            "#,
        )
        .fetch(pool)
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for MobileHotspotInfo {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        // device_type came as TEXT from `::text` on jsonb; it may look like `"wifiIndoor"`
        let dt_raw: String = row.try_get("device_type")?;
        let dt_clean = dt_raw.trim_matches('"'); // handle jsonb -> text of a JSON string
        let gateway_type =
            GatewayType::from_str(dt_clean).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

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
            owner: row.get::<Option<String>, &str>("owner"),
            gateway_type,
            deployment_info,
        })
    }
}
