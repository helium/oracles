use crate::{gateway::db::Gateway, region_map};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::iot_config::{
        GatewayInfo as GatewayInfoProto, GatewayMetadata as GatewayMetadataProto,
    },
    Region,
};
use sqlx::PgExecutor;

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

// Hotspot gain default; dbi * 10
const DEFAULT_GAIN: u32 = 12;
// Hotspot elevation default; meters above sea level
const DEFAULT_ELEVATION: u32 = 0;

pub async fn get(
    db: impl PgExecutor<'_>,
    address: &PublicKeyBinary,
) -> anyhow::Result<Option<IotMetadata>> {
    let gateway = Gateway::get_by_address(db, address).await?;
    Ok(gateway.map(IotMetadata::from))
}

pub fn stream<'a>(
    db: impl PgExecutor<'a> + 'a,
    min_last_changed_at: DateTime<Utc>,
    min_location_changed_at: Option<DateTime<Utc>>,
) -> impl Stream<Item = IotMetadata> + 'a {
    let stream = Gateway::stream(db, min_last_changed_at, min_location_changed_at);
    stream.map(IotMetadata::from).boxed()
}

#[derive(Clone, Debug)]
pub struct GatewayMetadata {
    pub location: u64,
    pub elevation: i32,
    pub gain: i32,
    pub region: Region,
}

#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadata>,
    pub is_full_hotspot: bool,
}

pub struct IotMetadata {
    pub address: PublicKeyBinary,
    pub location: Option<u64>,
    pub elevation: i32,
    pub gain: i32,
    pub is_full_hotspot: bool,
}

impl GatewayInfo {
    pub fn chain_metadata_to_info(
        meta: IotMetadata,
        region_map: &region_map::RegionMapReader,
    ) -> Self {
        let metadata = if let Some(location) = meta.location {
            if let Ok(region) = h3index_to_region(location, region_map) {
                Some(GatewayMetadata {
                    location,
                    elevation: meta.elevation,
                    gain: meta.gain,
                    region,
                })
            } else {
                tracing::debug!(
                    pubkey = meta.address.to_string(),
                    location,
                    "gateway region lookup failed for asserted location"
                );
                None
            }
        } else {
            None
        };

        Self {
            address: meta.address,
            is_full_hotspot: meta.is_full_hotspot,
            metadata,
        }
    }
}

fn h3index_to_region(
    location: u64,
    region_map: &region_map::RegionMapReader,
) -> anyhow::Result<Region> {
    hextree::Cell::from_raw(location)
        .map(|cell| region_map.get_region(cell))?
        .ok_or_else(|| anyhow!("invalid region"))
}

impl From<GatewayInfoProto> for GatewayInfo {
    fn from(info: GatewayInfoProto) -> Self {
        let metadata = if let Some(metadata) = info.metadata {
            u64::from_str_radix(&metadata.location, 16)
                .map(|location| GatewayMetadata {
                    location,
                    elevation: metadata.elevation,
                    gain: metadata.gain,
                    region: metadata.region(),
                })
                .ok()
        } else {
            None
        };
        Self {
            address: info.address.into(),
            is_full_hotspot: info.is_full_hotspot,
            metadata,
        }
    }
}

impl TryFrom<GatewayInfo> for GatewayInfoProto {
    type Error = hextree::Error;

    fn try_from(info: GatewayInfo) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            Some(GatewayMetadataProto {
                location: hextree::Cell::from_raw(metadata.location)?.to_string(),
                elevation: metadata.elevation,
                gain: metadata.gain,
                region: metadata.region.into(),
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            is_full_hotspot: info.is_full_hotspot,
            metadata,
        })
    }
}

impl From<Gateway> for IotMetadata {
    fn from(gateway: Gateway) -> Self {
        Self {
            address: gateway.address,
            location: gateway.location,
            elevation: gateway.elevation.unwrap_or(DEFAULT_ELEVATION) as i32,
            gain: gateway.gain.unwrap_or(DEFAULT_GAIN) as i32,
            is_full_hotspot: gateway.is_full_hotspot.unwrap_or(false),
        }
    }
}
