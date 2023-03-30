use futures::stream::BoxStream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::iot_config::GatewayInfo as GatewayInfoProto, Region};

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub location: Option<u64>,
    pub elevation: Option<i32>,
    pub gain: i32,
    pub is_full_hotspot: bool,
    pub region: Region,
}

#[async_trait::async_trait]
pub trait GatewayInfoResolver {
    type Error;

    async fn resolve_gateway_info(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, Self::Error>;

    async fn stream_gateway_info(&mut self) -> Result<GatewayInfoStream, Self::Error>;
}

impl From<GatewayInfoProto> for GatewayInfo {
    fn from(info: GatewayInfoProto) -> Self {
        let elevation = if info.elevation >= 0 { Some(info.elevation) } else { None };
        let region = info.region();
        Self {
            address: info.address.into(),
            location: u64::from_str_radix(&info.location, 16).ok(),
            elevation,
            gain: info.gain,
            is_full_hotspot: info.is_full_hotspot,
            region,
        }
    }
}

impl TryFrom<GatewayInfo> for GatewayInfoProto {
    type Error = hextree::Error;

    fn try_from(info: GatewayInfo) -> Result<Self, Self::Error> {
        let location = info.location.map_or(Ok(String::new()), |location| {
            Ok(hextree::Cell::from_raw(location)?.to_string())
        })?;
        Ok(Self {
            address: info.address.into(),
            location,
            elevation: info.elevation.unwrap_or(-1),
            gain: info.gain,
            is_full_hotspot: info.is_full_hotspot,
            region: info.region.into(),
        })
    }
}

pub(crate) mod db {
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{PgExecutor, Row};

    pub struct GatewayMetadata {
        pub address: PublicKeyBinary,
        pub location: Option<u64>,
        pub elevation: Option<i32>,
        pub gain: i32,
        pub is_full_hotspot: bool,
    }

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<GatewayMetadata>> {
        Ok(sqlx::query_as::<_, GatewayMetadata>(
            r#"
            select (hotspot_key, location, elevation, gain, is_full_hotspot) from iot_metadata
            where hotspot_key = $1
            "#,
        )
        .bind(address)
        .fetch_optional(db)
        .await?)
    }

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = GatewayMetadata> + 'a {
        sqlx::query_as::<_, GatewayMetadata>(
            r#"
            select (hotspot_key, location, elevation, gain, is_full_hotspot) from iot_metadata
            "#,
        )
        .fetch(db)
        .filter_map(|metadata| async move { metadata.ok() })
        .boxed()
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for GatewayMetadata {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            Ok(Self {
                address: row.get::<PublicKeyBinary, &str>("hotspot_key"),
                location: row.get::<Option<i64>, &str>("location").map(|v| v as u64),
                elevation: row.get::<Option<i32>, &str>("elevation"),
                gain: row.get("gain"),
                is_full_hotspot: row.get("is_full_hotspot"),
            })
        }
    }
}
