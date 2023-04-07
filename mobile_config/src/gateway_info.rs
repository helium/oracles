use futures::stream::BoxStream;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    GatewayInfo as GatewayInfoProto, GatewayMetadata as GatewayMetadataProto,
};

pub type GatewayInfoStream = BoxStream<'static, GatewayInfo>;

#[derive(Clone, Debug)]
pub struct GatewayMetadata {
    pub location: u64,
}

#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadata>,
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
                .map(|location| GatewayMetadata { location })
                .ok()
        } else {
            None
        };
        Self {
            address: info.address.into(),
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
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            metadata,
        })
    }
}

pub(crate) mod db {
    use super::{GatewayInfo, GatewayMetadata};
    use futures::stream::{Stream, StreamExt};
    use helium_crypto::PublicKeyBinary;
    use sqlx::{PgExecutor, Row};

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<GatewayInfo>> {
        Ok(sqlx::query_as::<_, GatewayInfo>(
            r#"
                select (hotspot_key, location) from mobile_metadata
                where hotspot_key = $1
                "#,
        )
        .bind(address)
        .fetch_optional(db)
        .await?)
    }

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = GatewayInfo> + 'a {
        sqlx::query_as::<_, GatewayInfo>(r#" select (hotspot_key, location) from mobile_metadata "#)
            .fetch(db)
            .filter_map(|metadata| async move { metadata.ok() })
            .boxed()
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for GatewayInfo {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            let metadata = row
                .get::<Option<i64>, &str>("location")
                .map(|loc| GatewayMetadata {
                    location: loc as u64,
                });
            Ok(Self {
                address: row.get::<PublicKeyBinary, &str>("hotspot_key"),
                metadata,
            })
        }
    }
}
