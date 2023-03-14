use futures::stream::{BoxStream, Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::HotspotMetadata as HotspotMetadataProto;
use sqlx::{PgExecutor, Row};

pub type HotspotMetadataStream = BoxStream<'static, HotspotMetadata>;

#[derive(Clone, Debug)]
pub struct HotspotMetadata {
    pub address: PublicKeyBinary,
    pub location: Option<u64>,
}

#[async_trait::async_trait]
pub trait HotspotMetadataResolver {
    type Error;

    async fn resolve_hotspot_metadata(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<HotspotMetadata>, Self::Error>;

    async fn stream_hotspots_metadata(&mut self) -> Result<HotspotMetadataStream, Self::Error>;
}

pub async fn get_metadata(
    db: impl PgExecutor<'_>,
    address: &PublicKeyBinary,
) -> anyhow::Result<Option<HotspotMetadata>> {
    Ok(sqlx::query_as::<_, HotspotMetadata>(
        r#"
            select (hotspot_key, location) from mobile_metadata
            where hotspot_key = $1
            "#,
    )
    .bind(address)
    .fetch_optional(db)
    .await?)
}

pub fn all_metadata_stream<'a>(
    db: impl PgExecutor<'a> + 'a,
) -> impl Stream<Item = HotspotMetadata> + 'a {
    sqlx::query_as::<_, HotspotMetadata>(r#" select (hotspot_key, location) from mobile_metadata "#)
        .fetch(db)
        .filter_map(|metadata| async move { metadata.ok() })
        .boxed()
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for HotspotMetadata {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        let location = row
            .get::<Option<&str>, &str>("location")
            .map(|loc| {
                u64::from_str_radix(loc, 16).map_err(|err| sqlx::Error::Decode(Box::new(err)))
            })
            .transpose()?;
        Ok(Self {
            address: row.get::<PublicKeyBinary, &str>("hotspot_key"),
            location,
        })
    }
}

impl From<HotspotMetadataProto> for HotspotMetadata {
    fn from(meta: HotspotMetadataProto) -> Self {
        let location = u64::from_str_radix(&meta.location, 16).ok();
        Self {
            address: meta.address.into(),
            location,
        }
    }
}

impl TryFrom<HotspotMetadata> for HotspotMetadataProto {
    type Error = hextree::Error;

    fn try_from(meta: HotspotMetadata) -> Result<Self, Self::Error> {
        let location = meta.location.map_or(Ok(String::new()), |location| {
            Ok(hextree::Cell::from_raw(location)?.to_string())
        })?;
        Ok(Self {
            address: meta.address.into(),
            location,
        })
    }
}
