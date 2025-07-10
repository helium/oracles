use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    // gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    // CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    // CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    // GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    // GatewayMetadata as GatewayMetadataProto, GatewayMetadataV2 as GatewayMetadataProtoV2,
    // WifiDeploymentInfo as WifiDeploymentInfoProto,
    DeviceTypeV2 as DeviceTypeProtoV2,
    GatewayInfoV3 as GatewayInfoProtoV3,
};

#[derive(Clone, Debug)]
pub struct GatewayMetadataV3 {
    pub location: u64,
    pub location_changed_at: DateTime<Utc>,
    pub antenna: u32,
    pub elevation: u32,
    pub azimuth: u32,
}

#[derive(Clone, Debug)]
pub enum DeviceTypeV2 {
    Indoor,
    Outdoor,
    DataOnly,
}

#[derive(Clone, Debug)]
pub struct GatewayInfoV3 {
    pub address: PublicKeyBinary,
    pub metadata: Option<GatewayMetadataV3>,
    pub device_type: DeviceTypeV2,
    pub created_at: DateTime<Utc>,
    // updated_at refers to the last time the data was actually changed.
    pub updated_at: DateTime<Utc>,
    // refreshed_at indicates the last time the chain was consulted, regardless of data changes.
    pub refreshed_at: DateTime<Utc>,
}

impl From<DeviceTypeProtoV2> for DeviceTypeV2 {
    fn from(value: DeviceTypeProtoV2) -> Self {
        match value {
            DeviceTypeProtoV2::Indoor => DeviceTypeV2::Indoor,
            DeviceTypeProtoV2::Outdoor => DeviceTypeV2::Outdoor,
            DeviceTypeProtoV2::DataOnly => DeviceTypeV2::DataOnly,
        }
    }
}

impl TryFrom<GatewayInfoV3> for GatewayInfoProtoV3 {
    type Error = hextree::Error;

    fn try_from(_info: GatewayInfoV3) -> Result<Self, Self::Error> {
        todo!()
        // let metadata = if let Some(metadata) = info.metadata {
        //     Some(GatewayMetadataProto {
        //         location: hextree::Cell::from_raw(metadata.location)?.to_string(),
        //     })
        // } else {
        //     None
        // };
        // Ok(Self {
        //     address: info.address.into(),
        //     metadata,
        //     device_type: info.device_type as i32,
        // })
    }
}

pub(crate) mod db {
    use chrono::{DateTime, Utc};
    use futures::stream::Stream;
    use futures::TryStreamExt;
    use helium_crypto::PublicKeyBinary;
    use sqlx::PgExecutor;
    use sqlx::Row;
    use std::{collections::HashMap, str::FromStr, sync::LazyLock};

    use super::{DeviceTypeV2, GatewayInfoV3};

    pub struct MobileTrackerInfo {
        location: Option<u64>,
        last_changed_at: DateTime<Utc>,
        asserted_location_changed_at: Option<DateTime<Utc>>,
    }
    pub type MobileTrackerInfoMap = HashMap<PublicKeyBinary, MobileTrackerInfo>;

    const GET_UPDATED_RADIOS: &str =
        "SELECT entity_key, last_changed_at, asserted_location, asserted_location_changed_at 
        FROM mobile_radio_tracker 
        WHERE last_changed_at >= $1 and asserted_location_changed_at >= $2";

    // TODO think how to handle asserted_location and asserted_location_changed_at NULLs
    pub async fn get_mobile_tracker_gateways_info(
        db: impl PgExecutor<'_>,
        min_updated_at: DateTime<Utc>,
        min_location_changed_at: DateTime<Utc>,
    ) -> anyhow::Result<MobileTrackerInfoMap> {
        sqlx::query(GET_UPDATED_RADIOS)
            .bind(min_updated_at)
            .bind(min_location_changed_at)
            .fetch(db)
            .map_err(anyhow::Error::from)
            .try_fold(
                MobileTrackerInfoMap::new(),
                |mut map: MobileTrackerInfoMap, row| async move {
                    let entity_key_b = row.get::<&[u8], &str>("entity_key");
                    let entity_key = bs58::encode(entity_key_b).into_string();
                    let last_changed_at = row.get::<DateTime<Utc>, &str>("last_changed_at");
                    let asserted_location_changed_at =
                        row.get::<Option<DateTime<Utc>>, &str>("asserted_location_changed_at ");
                    let location = row.get::<Option<i64>, &str>("location");

                    map.insert(
                        PublicKeyBinary::from_str(&entity_key)?,
                        MobileTrackerInfo {
                            location: location.map(|v| v as u64),
                            last_changed_at,
                            asserted_location_changed_at,
                        },
                    );
                    Ok(map)
                },
            )
            .await
    }

    pub fn all_info_stream_v3<'a>(
        _db: impl PgExecutor<'a> + 'a,
        device_types: &'a [DeviceTypeV2],
    ) -> impl Stream<Item = GatewayInfoV3> + 'a {
        // TODO
        futures::stream::empty::<GatewayInfoV3>()
    }
}
