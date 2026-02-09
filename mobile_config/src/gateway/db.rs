use crate::gateway::{
    metadata_db::MobileHotspotInfo,
    service::{info::DeploymentInfo, info::DeviceType, info_v3::DeviceTypeV2},
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{postgres::PgRow, FromRow, PgExecutor, PgPool, Postgres, QueryBuilder, Row};
use std::convert::TryFrom;
use strum::EnumIter;

// Postgres enum: gateway_type
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, EnumIter)]
#[sqlx(type_name = "gateway_type")]
pub enum GatewayType {
    #[sqlx(rename = "wifiIndoor")]
    WifiIndoor,
    #[sqlx(rename = "wifiOutdoor")]
    WifiOutdoor,
    #[sqlx(rename = "wifiDataOnly")]
    WifiDataOnly,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid device type string")]
pub struct GatewayTypeParseError;

impl std::fmt::Display for GatewayType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayType::WifiIndoor => write!(f, "wifiIndoor"),
            GatewayType::WifiOutdoor => write!(f, "wifiOutdoor"),
            GatewayType::WifiDataOnly => write!(f, "wifiDataOnly"),
        }
    }
}

impl std::str::FromStr for GatewayType {
    type Err = GatewayTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "wifiIndoor" => Self::WifiIndoor,
            "wifiOutdoor" => Self::WifiOutdoor,
            "wifiDataOnly" => Self::WifiDataOnly,
            _ => return Err(GatewayTypeParseError),
        };
        Ok(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayTypeError {
    #[error("CBRS gateways are not supported in the Gateway table")]
    CbrsUnsupported,
}

impl TryFrom<DeviceType> for GatewayType {
    type Error = GatewayTypeError;

    fn try_from(dt: DeviceType) -> Result<Self, Self::Error> {
        match dt {
            DeviceType::Cbrs => Err(GatewayTypeError::CbrsUnsupported),
            DeviceType::WifiIndoor => Ok(GatewayType::WifiIndoor),
            DeviceType::WifiOutdoor => Ok(GatewayType::WifiOutdoor),
            DeviceType::WifiDataOnly => Ok(GatewayType::WifiDataOnly),
        }
    }
}

impl From<DeviceTypeV2> for GatewayType {
    fn from(dt: DeviceTypeV2) -> Self {
        match dt {
            DeviceTypeV2::Indoor => GatewayType::WifiIndoor,
            DeviceTypeV2::Outdoor => GatewayType::WifiOutdoor,
            DeviceTypeV2::DataOnly => GatewayType::WifiDataOnly,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashParams {
    pub gateway_type: GatewayType,
    pub location: Option<u64>,
    pub antenna: Option<u32>,
    pub elevation: Option<u32>,
    pub azimuth: Option<u32>,
    pub location_asserts: Option<u32>,
    pub owner: Option<String>,
}

impl HashParams {
    pub fn from_hotspot_info(mhi: &MobileHotspotInfo) -> Self {
        let (antenna, elevation, azimuth) = match mhi.deployment_info {
            Some(ref info) => match info {
                DeploymentInfo::WifiDeploymentInfo(ref wifi) => {
                    (Some(wifi.antenna), Some(wifi.elevation), Some(wifi.azimuth))
                }
                DeploymentInfo::CbrsDeploymentInfo(_) => (None, None, None),
            },
            None => (None, None, None),
        };

        Self {
            gateway_type: mhi.gateway_type,
            location: mhi.location.map(|v| v as u64),
            antenna,
            elevation,
            azimuth,
            location_asserts: mhi.num_location_asserts.map(|n| n as u32),
            owner: mhi.owner.clone(),
        }
    }

    pub fn compute_hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.gateway_type.to_string().as_bytes());
        hasher.update(
            self.location
                .map(|l| l.to_le_bytes())
                .unwrap_or([0u8; 8])
                .as_ref(),
        );
        hasher.update(
            self.antenna
                .map(|v| v.to_le_bytes())
                .unwrap_or([0u8; 4])
                .as_ref(),
        );
        hasher.update(
            self.elevation
                .map(|v| v.to_le_bytes())
                .unwrap_or([0u8; 4])
                .as_ref(),
        );
        hasher.update(
            self.azimuth
                .map(|v| v.to_le_bytes())
                .unwrap_or([0u8; 4])
                .as_ref(),
        );
        hasher.update(
            self.location_asserts
                .map(|v| v.to_le_bytes())
                .unwrap_or([0u8; 4])
                .as_ref(),
        );
        hasher.update(self.owner.as_deref().unwrap_or_default().as_bytes());

        hasher.finalize().to_string()
    }
}

#[derive(Debug, Clone)]
pub struct Gateway {
    pub address: PublicKeyBinary,
    // When the record was first created from metadata DB
    pub created_at: DateTime<Utc>,
    // When location or hash last changed
    pub last_changed_at: DateTime<Utc>,
    pub hash: String,
    // When location last changed
    pub location_changed_at: Option<DateTime<Utc>>,
    pub owner_changed_at: Option<DateTime<Utc>>,
    pub hash_params: HashParams,
}

#[derive(Debug)]
pub struct LocationChangedAtUpdate {
    pub address: PublicKeyBinary,
    pub location_changed_at: DateTime<Utc>,
    pub location: u64,
}

impl Gateway {
    pub fn gateway_type(&self) -> GatewayType {
        self.hash_params.gateway_type
    }

    pub fn location(&self) -> Option<u64> {
        self.hash_params.location
    }

    pub fn antenna(&self) -> Option<u32> {
        self.hash_params.antenna
    }

    pub fn elevation(&self) -> Option<u32> {
        self.hash_params.elevation
    }

    pub fn azimuth(&self) -> Option<u32> {
        self.hash_params.azimuth
    }

    pub fn location_asserts(&self) -> Option<u32> {
        self.hash_params.location_asserts
    }

    pub fn owner(&self) -> Option<&str> {
        self.hash_params.owner.as_deref()
    }

    pub fn compute_hash(&self) -> String {
        self.hash_params.compute_hash()
    }

    pub fn from_mobile_hotspot_info(mhi: &MobileHotspotInfo) -> Self {
        let hash_params = HashParams::from_hotspot_info(mhi);
        let hash = hash_params.compute_hash();
        let refreshed_at = mhi.refreshed_at.unwrap_or_else(Utc::now);

        Self {
            address: mhi.entity_key.clone(),
            created_at: mhi.created_at,
            last_changed_at: refreshed_at,
            hash,
            location_changed_at: mhi.location.map(|_| refreshed_at),
            owner_changed_at: Some(refreshed_at),
            hash_params,
        }
    }

    pub fn new_if_changed(&self, mhi: &MobileHotspotInfo) -> Option<Gateway> {
        let hash_params = HashParams::from_hotspot_info(mhi);
        let new_hash = hash_params.compute_hash();

        if self.hash == new_hash {
            return None;
        }

        let refreshed_at = mhi.refreshed_at.unwrap_or_else(Utc::now);
        let loc_changed = mhi.location != self.location().map(|v| v as i64);
        let owner_changed = mhi.owner.is_some() && mhi.owner.as_deref() != self.owner();

        let location_changed_at = if loc_changed {
            Some(refreshed_at)
        } else {
            self.location_changed_at
        };

        let owner_changed_at = if owner_changed {
            Some(refreshed_at)
        } else {
            self.owner_changed_at
        };

        Some(Gateway {
            address: mhi.entity_key.clone(),
            created_at: mhi.created_at,
            last_changed_at: refreshed_at,
            hash: new_hash,
            location_changed_at,
            owner_changed_at,
            hash_params,
        })
    }

    pub async fn insert_bulk(pool: &PgPool, rows: &[Gateway]) -> anyhow::Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            ) ",
        );

        qb.push_values(rows, |mut b, g| {
            b.push_bind(g.address.as_ref())
                .push_bind(g.hash_params.gateway_type)
                .push_bind(g.created_at)
                .push_bind(g.last_changed_at)
                .push_bind(g.hash.as_str())
                .push_bind(g.hash_params.antenna.map(|v| v as i64))
                .push_bind(g.hash_params.elevation.map(|v| v as i64))
                .push_bind(g.hash_params.azimuth.map(|v| v as i64))
                .push_bind(g.hash_params.location.map(|v| v as i64))
                .push_bind(g.location_changed_at)
                .push_bind(g.hash_params.location_asserts.map(|v| v as i64))
                .push_bind(g.hash_params.owner.as_deref())
                .push_bind(g.owner_changed_at);
        });

        let res = qb.build().execute(pool).await?;
        Ok(res.rows_affected())
    }

    pub async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateways (
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13
            )
            "#,
        )
        .bind(self.address.as_ref())
        .bind(self.hash_params.gateway_type)
        .bind(self.created_at)
        .bind(self.last_changed_at)
        .bind(self.hash.as_str())
        .bind(self.hash_params.antenna.map(|v| v as i64))
        .bind(self.hash_params.elevation.map(|v| v as i64))
        .bind(self.hash_params.azimuth.map(|v| v as i64))
        .bind(self.hash_params.location.map(|v| v as i64))
        .bind(self.location_changed_at)
        .bind(self.hash_params.location_asserts.map(|v| v as i64))
        .bind(self.hash_params.owner.as_deref())
        .bind(self.owner_changed_at)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_by_address<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = $1
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(address.as_ref())
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub async fn get_by_addresses<'a>(
        db: impl PgExecutor<'a>,
        addresses: Vec<&PublicKeyBinary>,
    ) -> anyhow::Result<Vec<Self>> {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        let rows = sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = ANY($1)
            ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(addr_array)
        .fetch_all(db)
        .await?;

        Ok(rows)
    }

    pub async fn get_by_address_and_inserted_at<'a>(
        db: impl PgExecutor<'a>,
        address: &PublicKeyBinary,
        inserted_at_max: &DateTime<Utc>,
    ) -> anyhow::Result<Option<Self>> {
        let gateway = sqlx::query_as::<_, Self>(
            r#"
            SELECT
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = $1
            AND inserted_at <= $2
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(address.as_ref())
        .bind(inserted_at_max)
        .fetch_optional(db)
        .await?;

        Ok(gateway)
    }

    pub fn stream_by_addresses<'a>(
        db: impl PgExecutor<'a> + 'a,
        addresses: Vec<PublicKeyBinary>,
        min_last_changed_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                last_changed_at,
                hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = ANY($1)
                AND last_changed_at >= $2
            ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(addr_array)
        .bind(min_last_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    pub fn stream_by_types<'a>(
        db: impl PgExecutor<'a> + 'a,
        types: Vec<GatewayType>,
        min_last_changed_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT DISTINCT ON (address)
                    address,
                    gateway_type,
                    created_at,
                    last_changed_at,
                    hash,
                    antenna,
                    elevation,
                    azimuth,
                    location,
                    location_changed_at,
                    location_asserts,
                    owner,
                    owner_changed_at
                FROM gateways
                WHERE gateway_type = ANY($1)
                AND last_changed_at >= $2
                AND (
                    $3::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $3)
                )
                ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(types)
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    pub fn stream_gateway_info_v4<'a>(
        db: impl PgExecutor<'a> + 'a,
        types: Vec<GatewayType>,
        min_last_changed_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
        min_owner_changed_at: DateTime<Utc>,
    ) -> impl Stream<Item = Self> + 'a {
        sqlx::query_as::<_, Self>(
            r#"
                SELECT DISTINCT ON (address)
                    address,
                    gateway_type,
                    created_at,
                    last_changed_at,
                    hash,
                    antenna,
                    elevation,
                    azimuth,
                    location,
                    location_changed_at,
                    location_asserts,
                    owner,
                    owner_changed_at
                FROM gateways
                WHERE gateway_type = ANY($1)
                AND last_changed_at >= $2
                AND (
                    $3::timestamptz IS NULL
                    OR (location IS NOT NULL AND location_changed_at >= $3)
                )
                AND owner_changed_at >= $4
                AND owner IS NOT NULL
                ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(types)
        .bind(min_last_changed_at)
        .bind(min_location_changed_at)
        .bind(min_owner_changed_at)
        .fetch(db)
        .map_err(anyhow::Error::from)
        .filter_map(|res| async move { res.ok() })
    }

    // TODO Remove after migration
    pub async fn get_by_addresses_with_null_hash<'a>(
        db: impl PgExecutor<'a>,
        addresses: Vec<&PublicKeyBinary>,
    ) -> anyhow::Result<Vec<Self>> {
        let addr_array: Vec<Vec<u8>> = addresses.iter().map(|a| a.as_ref().to_vec()).collect();

        let rows = sqlx::query_as::<_, Self>(
            r#"
            SELECT DISTINCT ON (address)
                address,
                gateway_type,
                created_at,
                last_changed_at,
                COALESCE(hash, '') as hash,
                antenna,
                elevation,
                azimuth,
                location,
                location_changed_at,
                location_asserts,
                owner,
                owner_changed_at
            FROM gateways
            WHERE address = ANY($1)
              AND hash IS NULL
            ORDER BY address, inserted_at DESC
            "#,
        )
        .bind(addr_array)
        .fetch_all(db)
        .await?;

        Ok(rows)
    }

    // TODO Remove after migration
    pub async fn update_latest_hash(
        pool: &PgPool,
        address: &PublicKeyBinary,
        hash: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE gateways
            SET hash = $2
            WHERE address = $1
              AND inserted_at = (
                SELECT MAX(inserted_at) FROM gateways WHERE address = $1
              )
            "#,
        )
        .bind(address.as_ref())
        .bind(hash)
        .execute(pool)
        .await?;

        Ok(())
    }
}

impl FromRow<'_, PgRow> for Gateway {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        // helpers to map Option<i64> -> Option<u32/u64>
        let to_u32 = |v: Option<i64>| -> Option<u32> { v.map(|x| x as u32) };
        let to_u64 = |v: Option<i64>| -> Option<u64> { v.map(|x| x as u64) };

        Ok(Self {
            address: PublicKeyBinary::from(row.try_get::<Vec<u8>, _>("address")?),
            created_at: row.try_get("created_at")?,
            last_changed_at: row.try_get("last_changed_at")?,
            hash: row.try_get("hash")?,
            location_changed_at: row.try_get("location_changed_at")?,
            owner_changed_at: row.try_get("owner_changed_at")?,
            hash_params: HashParams {
                gateway_type: row.try_get("gateway_type")?,
                location: to_u64(row.try_get("location")?),
                antenna: to_u32(row.try_get("antenna")?),
                elevation: to_u32(row.try_get("elevation")?),
                azimuth: to_u32(row.try_get("azimuth")?),
                location_asserts: to_u32(row.try_get("location_asserts")?),
                owner: row.try_get("owner")?,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{metadata_db::MobileHotspotInfo, service::info::WifiDeploymentInfo};
    use chrono::{TimeZone, Utc};

    fn base_hash_params() -> HashParams {
        HashParams {
            gateway_type: GatewayType::WifiIndoor,
            location: Some(631_711_281_837_647_359),
            antenna: Some(10),
            elevation: Some(20),
            azimuth: Some(180),
            location_asserts: Some(3),
            owner: Some("owner_abc".to_string()),
        }
    }

    fn base_mhi(entity_key: PublicKeyBinary) -> MobileHotspotInfo {
        MobileHotspotInfo {
            entity_key,
            refreshed_at: Some(Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap()),
            created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            location: Some(631_711_281_837_647_359),
            is_full_hotspot: Some(true),
            num_location_asserts: Some(3),
            is_active: Some(true),
            dc_onboarding_fee_paid: Some(1_000_000),
            gateway_type: GatewayType::WifiIndoor,
            deployment_info: Some(DeploymentInfo::WifiDeploymentInfo(WifiDeploymentInfo {
                antenna: 10,
                elevation: 20,
                azimuth: 180,
                mechanical_down_tilt: 0,
                electrical_down_tilt: 0,
            })),
            owner: Some("owner_abc".to_string()),
        }
    }

    fn dummy_pubkey() -> PublicKeyBinary {
        PublicKeyBinary::from(vec![1u8; 33])
    }

    // ── Task 1: HashParams::compute_hash determinism and field sensitivity ──

    #[test]
    fn compute_hash_is_deterministic() {
        let params = base_hash_params();
        assert_eq!(params.compute_hash(), params.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_gateway_type_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.gateway_type = GatewayType::WifiOutdoor;
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_location_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.location = Some(999);
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_antenna_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.antenna = Some(99);
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_elevation_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.elevation = Some(99);
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_azimuth_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.azimuth = Some(99);
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_location_asserts_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.location_asserts = Some(99);
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_when_owner_changes() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.owner = Some("different_owner".to_string());
        assert_ne!(base.compute_hash(), changed.compute_hash());
    }

    #[test]
    fn compute_hash_differs_none_vs_some() {
        let base = base_hash_params();
        let mut changed = base.clone();
        changed.location = None;
        assert_ne!(base.compute_hash(), changed.compute_hash());

        let mut changed2 = base.clone();
        changed2.owner = None;
        assert_ne!(base.compute_hash(), changed2.compute_hash());
    }

    // ── Task 2: new_if_changed returns None when nothing changed ──

    #[test]
    fn new_if_changed_returns_none_when_unchanged() {
        let pk = dummy_pubkey();
        let mhi = base_mhi(pk.clone());

        let gateway = Gateway::from_mobile_hotspot_info(&mhi);
        assert!(gateway.new_if_changed(&mhi).is_none());
    }

    // ── Task 3: new_if_changed timestamp tracking ──

    #[test]
    fn new_if_changed_updates_location_changed_at_preserves_owner_changed_at() {
        let pk = dummy_pubkey();
        let mhi = base_mhi(pk.clone());
        let old_gateway = Gateway::from_mobile_hotspot_info(&mhi);

        let new_time = Utc.with_ymd_and_hms(2025, 7, 1, 12, 0, 0).unwrap();
        let mut changed_mhi = mhi.clone();
        changed_mhi.location = Some(999_999); // only location changes
        changed_mhi.refreshed_at = Some(new_time);

        let new_gw = old_gateway
            .new_if_changed(&changed_mhi)
            .expect("should detect location change");

        assert_eq!(new_gw.location_changed_at, Some(new_time));
        assert_eq!(new_gw.last_changed_at, new_time);
        assert_eq!(new_gw.owner_changed_at, old_gateway.owner_changed_at);
    }

    #[test]
    fn new_if_changed_updates_owner_changed_at_preserves_location_changed_at() {
        let pk = dummy_pubkey();
        let mhi = base_mhi(pk.clone());
        let old_gateway = Gateway::from_mobile_hotspot_info(&mhi);

        let new_time = Utc.with_ymd_and_hms(2025, 7, 1, 12, 0, 0).unwrap();
        let mut changed_mhi = mhi.clone();
        changed_mhi.owner = Some("new_owner".to_string()); // only owner changes
        changed_mhi.refreshed_at = Some(new_time);

        let new_gw = old_gateway
            .new_if_changed(&changed_mhi)
            .expect("should detect owner change");

        assert_eq!(new_gw.owner_changed_at, Some(new_time));
        assert_eq!(new_gw.last_changed_at, new_time);
        assert_eq!(new_gw.location_changed_at, old_gateway.location_changed_at);
    }

    // ── Task 6: new_if_changed owner change when old owner is None ──

    #[test]
    fn new_if_changed_detects_owner_none_to_some() {
        let pk = dummy_pubkey();
        let mut mhi = base_mhi(pk.clone());
        mhi.owner = None; // start with no owner
        let old_gateway = Gateway::from_mobile_hotspot_info(&mhi);
        assert!(old_gateway.owner().is_none());

        let new_time = Utc.with_ymd_and_hms(2025, 7, 1, 12, 0, 0).unwrap();
        let mut changed_mhi = mhi.clone();
        changed_mhi.owner = Some("new_owner".to_string());
        changed_mhi.refreshed_at = Some(new_time);

        let new_gw = old_gateway
            .new_if_changed(&changed_mhi)
            .expect("should detect owner appearing");

        assert_eq!(new_gw.owner(), Some("new_owner"));
        assert_eq!(new_gw.owner_changed_at, Some(new_time));
    }
}
