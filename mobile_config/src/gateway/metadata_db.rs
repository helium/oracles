//! Helpers against the on-chain metadata Postgres (`mobile_hotspot_infos`,
//! `key_to_assets`).
//!
//! The dbt chain tables (dbt.mobile_gateway_inventory / mobile_hotspot_history)
//! carry every gateway field except the WiFi `(antenna, elevation)` pair, which
//! lives only in `mobile_hotspot_infos.deployment_info`. [`fetch_all_deployment_info`]
//! bulk-reads that pair for every mobile hotspot so the
//! [`crate::gateway::tracker::DeploymentInfoTracker`] can cache it locally in
//! the `deployment_info` table, which gateway reads then join against.

use crate::gateway::service::info::DeploymentInfo;
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres, Row};
use std::str::FromStr;

/// One entity's cached deployment values, ready to upsert into `deployment_info`.
#[derive(Debug, Clone)]
pub struct DeploymentInfoRecord {
    pub address: PublicKeyBinary,
    pub antenna: u32,
    pub elevation: u32,
}

/// Bulk-fetch `(antenna, elevation)` for every mobile hotspot that has WiFi
/// deployment info. Entities with no row, non-WiFi (CBRS) deployment, or
/// missing/empty deployment info are simply absent from the result.
pub async fn fetch_all_deployment_info(
    pool: &Pool<Postgres>,
) -> sqlx::Result<Vec<DeploymentInfoRecord>> {
    let rows = sqlx::query(
        r#"
            SELECT
                DISTINCT ON (kta.entity_key)
                kta.entity_key,
                mhi.deployment_info::text AS deployment_info
            FROM key_to_assets kta
            INNER JOIN mobile_hotspot_infos mhi ON kta.asset = mhi.asset
            ORDER BY kta.entity_key, mhi.refreshed_at DESC NULLS LAST
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let bytes: &[u8] = row.try_get("entity_key").ok()?;
            let address = PublicKeyBinary::from_str(&bs58::encode(bytes).into_string()).ok()?;
            let (antenna, elevation) = parse_wifi_deployment(&row, "deployment_info");
            Some(DeploymentInfoRecord {
                address,
                antenna: antenna?,
                elevation: elevation?,
            })
        })
        .collect())
}

fn parse_wifi_deployment(row: &sqlx::postgres::PgRow, column: &str) -> (Option<u32>, Option<u32>) {
    let deployment_info: Option<DeploymentInfo> = match row.try_get::<Option<String>, _>(column) {
        Ok(Some(s)) if !s.is_empty() => serde_json::from_str(&s).ok(),
        _ => None,
    };

    match deployment_info {
        Some(DeploymentInfo::WifiDeploymentInfo(wifi)) => {
            (Some(wifi.antenna), Some(wifi.elevation))
        }
        _ => (None, None),
    }
}
