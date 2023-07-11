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
        &self,
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
    use std::str::FromStr;

    const GET_METADATA_SQL: &str = r#"
            select kta.entity_key, infos.location::bigint
            from mobile_hotspot_infos infos
            join key_to_assets kta on infos.asset = kta.asset
        "#;

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        address: &PublicKeyBinary,
    ) -> anyhow::Result<Option<GatewayInfo>> {
        let entity_key = bs58::decode(address.to_string()).into_vec()?;
        let mut query: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(GET_METADATA_SQL);
        query.push(" where kta.entity_key = $1 ");
        Ok(query
            .build_query_as::<GatewayInfo>()
            .bind(entity_key)
            .fetch_optional(db)
            .await?)
    }

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = GatewayInfo> + 'a {
        sqlx::query_as::<_, GatewayInfo>(GET_METADATA_SQL)
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
                address: PublicKeyBinary::from_str(
                    &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
                )
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
                metadata,
            })
        }
    }
}
