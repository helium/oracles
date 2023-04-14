use crate::region_map;
use anyhow::anyhow;
use futures::stream::BoxStream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::iot_config::{
        GatewayInfo as GatewayInfoProto, GatewayMetadata as GatewayMetadataProto,
    },
    Region,
};

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

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

impl GatewayInfo {
    pub fn chain_metadata_to_info(
        meta: db::IotMetadata,
        region_map: &region_map::RegionMapReader,
    ) -> Self {
        let metadata = if let (Some(location), Some(elevation), Some(gain)) =
            (meta.location, meta.elevation, meta.gain)
        {
            if let Ok(region) = h3index_to_region(location, region_map) {
                Some(GatewayMetadata {
                    location,
                    elevation,
                    gain,
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

#[async_trait::async_trait]
pub trait GatewayInfoResolver {
    type Error;

    async fn resolve_gateway_info(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, Self::Error>;

    async fn stream_gateways_info(&mut self) -> Result<GatewayInfoStream, Self::Error>;
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

pub(crate) mod db {
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{PgExecutor, Row};

    pub struct IotMetadata {
        pub address: PublicKeyBinary,
        pub location: Option<u64>,
        pub elevation: Option<i32>,
        pub gain: Option<i32>,
        pub is_full_hotspot: bool,
    }

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<IotMetadata>> {
        Ok(sqlx::query_as::<_, IotMetadata>(
            r#"
            select hotspot_key::text, location, elevation, gain, is_full_hotspot from iot_metadata
            where hotspot_key = $1
            "#,
        )
        .bind(address)
        .fetch_optional(db)
        .await?)
    }

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = IotMetadata> + 'a {
        sqlx::query_as::<_, IotMetadata>(
            r#"
            select hotspot_key::text, location, elevation, gain, is_full_hotspot from iot_metadata
            "#,
        )
        .fetch(db)
        .filter_map(|metadata| async move { metadata.ok() })
        .boxed()
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for IotMetadata {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            Ok(Self {
                address: row.get::<PublicKeyBinary, &str>("hotspot_key"),
                location: row.get::<Option<i64>, &str>("location").map(|v| v as u64),
                elevation: row.get::<Option<i32>, &str>("elevation"),
                gain: row.get::<Option<i32>, &str>("gain"),
                is_full_hotspot: row.get("is_full_hotspot"),
            })
        }
    }
}
