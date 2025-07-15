use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    // gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    // CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    // CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    // GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    // GatewayMetadata as GatewayMetadataProto, GatewayMetadataV2 as GatewayMetadataProtoV2,
    // WifiDeploymentInfo as WifiDeploymentInfoProto,
    DeploymentInfo as DeploymentInfoProto,
    DeviceTypeV2 as DeviceTypeProtoV2,
    GatewayInfoV3 as GatewayInfoProtoV3,
    LocationInfo as LocationInfoProto,
};

use crate::gateway_info::DeviceTypeParseError;

#[derive(Clone, Debug)]
pub struct LocationInfo {
    pub location: u64,
    pub location_changed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct GatewayMetadataV3 {
    pub location_info: LocationInfo,
    pub deployment_info: Option<DeploymentInfoProto>,
}

#[derive(Clone, Debug)]
pub enum DeviceTypeV2 {
    Indoor,
    Outdoor,
    DataOnly,
}

impl std::str::FromStr for DeviceTypeV2 {
    type Err = DeviceTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "wifiIndoor" => Self::Indoor,
            "wifiOutdoor" => Self::Outdoor,
            "wifiDataOnly" => Self::DataOnly,
            _ => return Err(DeviceTypeParseError),
        };
        Ok(result)
    }
}

impl DeviceTypeV2 {
    fn to_sql_param(&self) -> &'static str {
        match self {
            DeviceTypeV2::Indoor => "wifiIndoor",
            DeviceTypeV2::Outdoor => "wifiOutdoor",
            DeviceTypeV2::DataOnly => "wifiDataOnly",
        }
    }
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
    use futures::{
        stream::{Stream, StreamExt},
        TryStreamExt,
    };
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::mobile_config::DeploymentInfo as DeploymentInfoProto;
    use sqlx::Row;
    use sqlx::{types::Json, PgExecutor};
    use std::{collections::HashMap, str::FromStr, sync::LazyLock};

    use super::{DeviceTypeV2, GatewayInfoV3, GatewayMetadataV3};

    pub struct MobileTrackerInfo {
        location: Option<u64>,
        last_changed_at: DateTime<Utc>,
        asserted_location_changed_at: Option<DateTime<Utc>>,
    }
    pub type MobileTrackerInfoMap = HashMap<PublicKeyBinary, MobileTrackerInfo>;

    // TODO test and add indexes if needed
    const GET_UPDATED_RADIOS: &str =
        "SELECT entity_key, last_changed_at, asserted_location, asserted_location_changed_at 
        FROM mobile_radio_tracker WHERE last_changed_at >= $1";

    static GET_UPDATED_RADIOS_WITH_LOCATION: LazyLock<String> = LazyLock::new(|| {
        format!("{GET_UPDATED_RADIOS} AND asserted_location IS NOT NULL AND asserted_location_changed_at >= $2")
    });

    const GET_MOBILE_HOTSPOT_INFO: &str = r#"
            SELECT kta.entity_key, infos.device_type, infos.refreshed_at, infos.created_at, infos.deployment_info
            FROM mobile_hotspot_infos infos
            JOIN key_to_assets kta ON infos.asset = kta.asset
            WHERE device_type != '"cbrs"'
        "#;
    const DEVICE_TYPES_WHERE_SNIPPET: &str = " where device_type::text = any($1) ";
    static DEVICE_TYPES_METADATA_SQL: LazyLock<String> =
        LazyLock::new(|| format!("{GET_MOBILE_HOTSPOT_INFO} {DEVICE_TYPES_WHERE_SNIPPET}"));

    pub async fn get_mobile_tracker_gateways_info(
        db: impl PgExecutor<'_>,
        min_updated_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<MobileTrackerInfoMap> {
        // TODO refactor
        if let Some(min_loc_changed_at) = min_location_changed_at {
            sqlx::query(&GET_UPDATED_RADIOS_WITH_LOCATION)
                .bind(min_updated_at)
                .bind(min_loc_changed_at)
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
                        let asserted_location = row.get::<i64, &str>("asserted_location");

                        map.insert(
                            PublicKeyBinary::from_str(&entity_key)?,
                            MobileTrackerInfo {
                                location: Some(asserted_location as u64),
                                last_changed_at,
                                asserted_location_changed_at,
                            },
                        );
                        Ok(map)
                    },
                )
                .await
        } else {
            sqlx::query(GET_UPDATED_RADIOS)
                .bind(min_updated_at)
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
                        let asserted_location = row.get::<Option<i64>, &str>("asserted_location");

                        map.insert(
                            PublicKeyBinary::from_str(&entity_key)?,
                            MobileTrackerInfo {
                                location: asserted_location.map(|v| v as u64),
                                last_changed_at,
                                asserted_location_changed_at,
                            },
                        );
                        Ok(map)
                    },
                )
                .await
        }
    }

    pub fn all_info_stream_v3<'a>(
        db: impl PgExecutor<'a> + 'a,
        device_types: &'a [DeviceTypeV2],
        mtim: &'a MobileTrackerInfoMap,
    ) -> impl Stream<Item = GatewayInfoV3> + 'a {
        match device_types.is_empty() {
            true => sqlx::query(GET_MOBILE_HOTSPOT_INFO)
                .fetch(db)
                .filter_map(move |hs_info| async move {
                    match hs_info {
                        Ok(info_row) => {
                            let address = PublicKeyBinary::from_str(
                                &bs58::encode(info_row.get::<&[u8], &str>("entity_key"))
                                    .into_string(),
                            )
                            .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                            .unwrap(); // TODO remove unwrap()

                            match mtim.get(&address) {
                                Some(mti) => {
                                    // TODO test location is Some but asserted_location_changed_at
                                    // is None
                                    let location = mti.location;
                                    let metadata = if let Some(loc) = location {
                                        // If location is Some, asserted_location_changed_at must
                                        // also be some. Otherwise, data is corrupted
                                        let asserted_location_changed_at =
                                            mti.asserted_location_changed_at?;

                                        // TODO function getWifiDeploymentInfo
                                        let deployment_info = match info_row.try_get::<Option<
                                            Json<crate::gateway_info::DeploymentInfo>,
                                        >, &str>(
                                            "deployment_info"
                                        ) {
                                            Ok(di) => di.map(|v| v.0),
                                            // We shouldn't fail if an error occurs in this case.
                                            // This is because the data in this column could be inconsistent,
                                            // and we don't want to break
                                            Err(_e) => None,
                                        };
                                        let deployment_info = match deployment_info {
                                            Some(di) => match di {
                                                crate::gateway_info::DeploymentInfo::WifiDeploymentInfo(wdi) => {
                                                    Some(DeploymentInfoProto {
                                                        antenna: wdi.antenna,
                                                        elevation: wdi.elevation,
                                                        azimuth: wdi.azimuth,
                                                    })
                                                }
                                                crate::gateway_info::DeploymentInfo::CbrsDeploymentInfo(_cdi) => None,
                                            },
                                            None => None,
                                        };

                                        Some(GatewayMetadataV3 {
                                            location_info: super::LocationInfo {
                                                location: loc,
                                                location_changed_at: asserted_location_changed_at,
                                            },
                                            deployment_info,
                                        })
                                    } else {
                                        None
                                    };
                                    let device_type = DeviceTypeV2::from_str(
                        info_row.get::<Json<String>, &str>("device_type")
                            .to_string()
                            .as_ref(),
                    )
                    .map_err(|err| sqlx::Error::Decode(Box::new(err))).unwrap();// todo remove unwrap
                    let created_at = info_row.get::<DateTime<Utc>, &str>("created_at");
                    let refreshed_at = info_row.get::<DateTime<Utc>, &str>("refreshed_at");

                                    Some(GatewayInfoV3 {
                                        address,
                                        metadata,
                                        device_type,
                                        created_at,
                                        refreshed_at,
                                        updated_at: mti.last_changed_at
                                    })
                                }
                                None => None,
                            }
                        }
                        Err(e) => {
                            tracing::error!("SLQX error during fetching mobile hotspot infos. {e}");
                            None
                        }
                    }
                })
                .boxed(),
            false => todo!(), // false => sqlx::query_as::<_, GatewayInfoV3>(&DEVICE_TYPES_METADATA_SQL)
                              //     .bind(
                              //         device_types
                              //             .iter()
                              //             // The device_types field has a jsonb type but is being used as a string,
                              //             // which forces us to add quotes.
                              //             .map(|v| format!("\"{}\"", v.to_sql_param()))
                              //             .collect::<Vec<_>>(),
                              //     )
                              //     .fetch(db)
                              //     .filter_map(|gwinfo| async move { gwinfo.ok() })
                              //     .boxed(),
        }
    }
}
