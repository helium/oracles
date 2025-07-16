use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::{
    // WifiDeploymentInfo as WifiDeploymentInfoProto,
    DeploymentInfo as DeploymentInfoProto,
    DeviceTypeV2 as DeviceTypeProtoV2,
    GatewayInfoV3 as GatewayInfoProtoV3,
    // gateway_metadata_v2::DeploymentInfo as DeploymentInfoProto,
    // CbrsDeploymentInfo as CbrsDeploymentInfoProto,
    // CbrsRadioDeploymentInfo as CbrsRadioDeploymentInfoProto, DeviceType as DeviceTypeProto,
    // GatewayInfo as GatewayInfoProto, GatewayInfoV2 as GatewayInfoProtoV2,
    GatewayMetadataV3 as GatewayMetadataProtoV3,
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

    fn try_from(info: GatewayInfoV3) -> Result<Self, Self::Error> {
        let metadata = if let Some(metadata) = info.metadata {
            let location_info = LocationInfoProto {
                location: hextree::Cell::from_raw(metadata.location_info.location)?.to_string(),
                location_changed_at: metadata.location_info.location_changed_at.timestamp() as u64,
            };
            let deployment_info = metadata.deployment_info.map(|di| DeploymentInfoProto {
                antenna: di.antenna,
                elevation: di.elevation,
                azimuth: di.azimuth,
            });

            Some(GatewayMetadataProtoV3 {
                location_info: Some(location_info),
                deployment_info,
            })
        } else {
            None
        };
        Ok(Self {
            address: info.address.into(),
            metadata,
            device_type: info.device_type as i32,
            created_at: info.created_at.timestamp() as u64,
            updated_at: info.updated_at.timestamp() as u64,
        })
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

    use crate::gateway_info::DeploymentInfo;

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
    const DEVICE_TYPES_WHERE_SNIPPET: &str = " AND device_type::text = any($1) ";
    static DEVICE_TYPES_METADATA_SQL: LazyLock<String> =
        LazyLock::new(|| format!("{GET_MOBILE_HOTSPOT_INFO} {DEVICE_TYPES_WHERE_SNIPPET}"));

    pub async fn get_mobile_tracker_gateways_info(
        db: impl PgExecutor<'_>,
        min_updated_at: DateTime<Utc>,
        min_location_changed_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<MobileTrackerInfoMap> {
        let query = if let Some(min_loc) = min_location_changed_at {
            sqlx::query(&GET_UPDATED_RADIOS_WITH_LOCATION)
                .bind(min_updated_at)
                .bind(min_loc)
        } else {
            sqlx::query(GET_UPDATED_RADIOS).bind(min_updated_at)
        };

        query
            .fetch(db)
            .map_err(anyhow::Error::from)
            .try_fold(
                MobileTrackerInfoMap::new(),
                |mut map: MobileTrackerInfoMap, row| async move {
                    let entity_key_b = row.get::<&[u8], &str>("entity_key");
                    let entity_key = bs58::encode(entity_key_b).into_string();
                    let last_changed_at = row.get::<DateTime<Utc>, &str>("last_changed_at");
                    let asserted_location_changed_at =
                        row.get::<Option<DateTime<Utc>>, &str>("asserted_location_changed_at");
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

    /// Streams all gateway info records, optionally filtering by device types.
    pub fn all_info_stream_v3<'a>(
        db: impl PgExecutor<'a> + 'a,
        device_types: &'a [DeviceTypeV2],
        mtim: &'a MobileTrackerInfoMap,
    ) -> impl Stream<Item = GatewayInfoV3> + 'a {
        // Choose base query depending on whether filtering is needed.
        let query = if device_types.is_empty() {
            sqlx::query(GET_MOBILE_HOTSPOT_INFO)
        } else {
            sqlx::query(&DEVICE_TYPES_METADATA_SQL).bind(
                device_types
                    .iter()
                    // The device_types field has a jsonb type but is being used as a string,
                    // which forces us to add quotes.
                    .map(|v| format!("\"{}\"", v.to_sql_param()))
                    .collect::<Vec<_>>(),
            )
        };

        query
            .fetch(db)
            .filter_map(move |result| async move {
                match result {
                    Ok(row) => process_row(row, mtim).await,
                    Err(e) => {
                        tracing::error!("SQLx fetch error: {e:?}");
                        None
                    }
                }
            })
            .boxed()
    }

    /// Processes a single database row into a GatewayInfoV3, returning None if any step fails.
    async fn process_row(
        row: sqlx::postgres::PgRow,
        mtim: &MobileTrackerInfoMap,
    ) -> Option<GatewayInfoV3> {
        let device_type = DeviceTypeV2::from_str(
            row.get::<Json<String>, &str>("device_type")
                .to_string()
                .as_ref(),
        )
        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
        .unwrap(); // TODO REMOVE

        let address = PublicKeyBinary::from_str(
            &bs58::encode(row.get::<&[u8], &str>("entity_key")).into_string(),
        )
        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
        .unwrap(); // TODO REMOVE

        let mti = mtim.get(&address)?;

        let updated_at = mti.last_changed_at;

        let metadata = mti.location.and_then(|loc| {
            let location_changed_at = mti.asserted_location_changed_at?;
            // Safely parse deployment_info JSON
            let deployment_info = row
                .try_get::<Option<sqlx::types::Json<DeploymentInfo>>, _>("deployment_info")
                .ok()
                .flatten()
                .and_then(|json| match json.0 {
                    DeploymentInfo::WifiDeploymentInfo(wdi) => Some(DeploymentInfoProto {
                        antenna: wdi.antenna,
                        elevation: wdi.elevation,
                        azimuth: wdi.azimuth,
                    }),
                    _ => None,
                });

            Some(GatewayMetadataV3 {
                location_info: super::LocationInfo {
                    location: loc,
                    location_changed_at,
                },
                deployment_info,
            })
        });

        let created_at: DateTime<Utc> = row.get("created_at");
        let refreshed_at: DateTime<Utc> = row.get("refreshed_at");

        Some(GatewayInfoV3 {
            address,
            metadata,
            device_type,
            created_at,
            refreshed_at,
            updated_at,
        })
    }
}
