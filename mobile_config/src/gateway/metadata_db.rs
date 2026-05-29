//! Single-purpose helpers against the on-chain metadata Postgres
//! (`mobile_hotspot_infos`, `key_to_assets`).
//!
//! The chain_rewardable_entities S3 stream is the primary source of gateway
//! updates. These helpers exist solely to recover the WiFi `(antenna,
//! elevation)` pair, which is not carried by the proto and must be looked
//! up from `mobile_hotspot_infos.deployment_info`.

use crate::gateway::service::info::DeploymentInfo;
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres, Row};
use std::{collections::HashMap, str::FromStr};

/// Single-row variant used by the hotspot change stream daemon — looks up
/// `(antenna, elevation)` for one entity at change-event time.
///
/// Returns `(None, None)` when the metadata DB does not yet have a row for
/// the entity, or when the row's `deployment_info` is missing/CBRS. The
/// reconciliation [`crate::gateway::tracker::Tracker`] backfills any nulls
/// left behind.
pub async fn fetch_antenna_and_elevation(
    pool: &Pool<Postgres>,
    entity_key: &PublicKeyBinary,
) -> sqlx::Result<(Option<u32>, Option<u32>)> {
    let entity_key_bytes = entity_key_storage_form(entity_key)?;

    let row = sqlx::query(
        r#"
            SELECT mhi.deployment_info::text AS deployment_info
            FROM key_to_assets kta
            INNER JOIN mobile_hotspot_infos mhi ON kta.asset = mhi.asset
            WHERE kta.entity_key = $1
            ORDER BY mhi.refreshed_at DESC NULLS LAST
            LIMIT 1
        "#,
    )
    .bind(entity_key_bytes)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok((None, None));
    };

    Ok(parse_wifi_deployment(&row, "deployment_info"))
}

/// Batched variant used by the reconciliation tracker — fetches
/// `(antenna, elevation)` for many entities in a single query.
///
/// Entities with no matching row in `key_to_assets` / `mobile_hotspot_infos`
/// are simply absent from the returned map.
pub async fn fetch_antenna_and_elevation_batch(
    pool: &Pool<Postgres>,
    entity_keys: &[PublicKeyBinary],
) -> sqlx::Result<HashMap<PublicKeyBinary, (Option<u32>, Option<u32>)>> {
    if entity_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let storage_keys: Vec<Vec<u8>> = entity_keys
        .iter()
        .map(entity_key_storage_form)
        .collect::<sqlx::Result<_>>()?;

    let rows = sqlx::query(
        r#"
            SELECT
                DISTINCT ON (kta.entity_key)
                kta.entity_key,
                mhi.deployment_info::text AS deployment_info
            FROM key_to_assets kta
            INNER JOIN mobile_hotspot_infos mhi ON kta.asset = mhi.asset
            WHERE kta.entity_key = ANY($1)
            ORDER BY kta.entity_key, mhi.refreshed_at DESC NULLS LAST
        "#,
    )
    .bind(storage_keys)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let bytes: &[u8] = row.try_get("entity_key")?;
            let pkb = PublicKeyBinary::from_str(&bs58::encode(bytes).into_string())
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let pair = parse_wifi_deployment(&row, "deployment_info");
            Ok((pkb, pair))
        })
        .collect()
}

/// `key_to_assets.entity_key` is stored as the bs58check-decoded form
/// (network byte + inner key + 4-byte checksum), not the raw inner bytes
/// returned by `PublicKeyBinary::as_ref()`. Round-trip through the bs58
/// string to match the storage encoding.
fn entity_key_storage_form(entity_key: &PublicKeyBinary) -> sqlx::Result<Vec<u8>> {
    bs58::decode(entity_key.to_string())
        .into_vec()
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))
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
