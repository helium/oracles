use super::{
    DeviceTypeCounts, GetHotspot, GetHotspotAtTimestamp, GetHotspotBatch, InfoStreamV4,
    PathBufKeypair,
};
use crate::{client, Msg, PrettyJson, Result};
use angry_purple_tiger::AnimalName;
use futures::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::services::mobile_config::{
    DeviceTypeV2, GatewayInfoStreamResV3, GatewayInfoV2 as GatewayInfoProto,
    GatewayMetadataV2 as GatewayMetadataProto,
};
use mobile_config::gateway::service::info::{DeploymentInfo, DeviceType};
use serde::Serialize;
use std::{collections::HashMap, str::FromStr};

pub type GatewayInfoStream = futures::stream::BoxStream<'static, GatewayInfo>;
pub type GatewayInfoStreamV3 = futures::stream::BoxStream<'static, GatewayInfoStreamResV3>;
pub type GatewayInfoStreamV4 =
    futures::stream::BoxStream<'static, helium_proto::services::mobile_config::GatewayInfoStreamResV4>;

#[derive(Debug, Serialize)]
pub struct GatewayInfo {
    name: String,
    pubkey: PublicKey,
    metadata: Option<GatewayMetadata>,
    device_type: DeviceType,
}

#[derive(Debug, Serialize)]
pub struct GatewayMetadata {
    location: String,
    deployment_info: Option<DeploymentInfo>,
    lat: f64,
    lon: f64,
}

pub async fn info(args: GetHotspot) -> Result<Msg> {
    let mut client = client::GatewayClient::new(&args.config_host, &args.config_pubkey).await?;
    match client
        .info(&args.hotspot, &args.keypair.to_keypair()?)
        .await
    {
        Ok(info) => Msg::ok(info.pretty_json()?),
        Err(err) => Msg::err(format!(
            "failed to retrieve {} info: {}",
            &args.hotspot, err
        )),
    }
}

pub async fn info_batch(args: GetHotspotBatch) -> Result<Msg> {
    let mut client = client::GatewayClient::new(&args.config_host, &args.config_pubkey).await?;
    match client
        .info_batch(&args.hotspot, args.batch_size, &args.keypair.to_keypair()?)
        .await
    {
        Ok(info_stream) => {
            let gateways = info_stream.collect::<Vec<GatewayInfo>>().await;
            Msg::ok(gateways.pretty_json()?)
        }
        Err(err) => Msg::err(format!(
            "failed to retrieve {:?} info: {}",
            &args.hotspot, err
        )),
    }
}

pub async fn info_at_timestamp(args: GetHotspotAtTimestamp) -> Result<Msg> {
    let mut client = client::GatewayClient::new(&args.config_host, &args.config_pubkey).await?;
    match client
        .info_at_timestamp(&args.hotspot, args.query_time, &args.keypair.to_keypair()?)
        .await
    {
        Ok(info) => Msg::ok(info.pretty_json()?),
        Err(err) => Msg::err(format!(
            "failed to retrieve {} info: {}",
            &args.hotspot, err
        )),
    }
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = anyhow::Error;

    fn try_from(info: GatewayInfoProto) -> Result<Self, Self::Error> {
        let device_type: DeviceType = info.device_type().into();
        let pubkey = PublicKey::try_from(info.address)?;
        let name: AnimalName = pubkey.clone().into();
        let metadata = if let Some(md) = info.metadata {
            Some(md.try_into()?)
        } else {
            None
        };
        Ok(Self {
            name: name.to_string(),
            pubkey,
            metadata,
            device_type,
        })
    }
}

impl TryFrom<GatewayMetadataProto> for GatewayMetadata {
    type Error = h3o::error::InvalidCellIndex;

    fn try_from(md: GatewayMetadataProto) -> Result<Self, Self::Error> {
        let location = md.clone().location;
        let latlng: h3o::LatLng = h3o::CellIndex::from_str(&md.location)?.into();
        Ok(Self {
            location,
            lat: latlng.lat(),
            lon: latlng.lng(),
            deployment_info: md.deployment_info.map(DeploymentInfo::from),
        })
    }
}

pub async fn device_type_counts(args: DeviceTypeCounts) -> Result<Msg> {
    let mut client = client::GatewayClient::new(&args.config_host, &args.config_pubkey).await?;
    let mut stream = client
        .info_stream_v3(args.batch_size, &args.keypair.to_keypair()?)
        .await?;

    let mut counts: HashMap<String, u64> = HashMap::new();

    while let Some(response) = stream.next().await {
        for gateway in response.gateways {
            let device_type = DeviceTypeV2::try_from(gateway.device_type)
                .map(|dt| format!("{dt:?}"))
                .unwrap_or_else(|_| format!("Unknown({})", gateway.device_type));
            *counts.entry(device_type).or_default() += 1;
        }
    }

    let total: u64 = counts.values().sum();
    let output = serde_json::json!({
        "counts": counts,
        "total": total
    });

    Msg::ok(serde_json::to_string_pretty(&output)?)
}

pub async fn info_stream_v4(args: InfoStreamV4) -> Result<Msg> {
    let mut client = client::GatewayClient::new(&args.config_host, &args.config_pubkey).await?;
    let mut stream = client
        .info_stream_v4(
            args.batch_size,
            args.min_owner_changed_at,
            &args.keypair.to_keypair()?,
        )
        .await?;

    let mut gateways = Vec::new();
    while let Some(response) = stream.next().await {
        for gateway in response.gateways {
            let device_type = DeviceTypeV2::try_from(gateway.device_type)
                .map(|dt| format!("{dt:?}"))
                .unwrap_or_else(|_| format!("Unknown({})", gateway.device_type));
            let address = PublicKey::try_from(gateway.address)
                .map(|pk| pk.to_string())
                .unwrap_or_else(|_| "invalid".to_string());
            gateways.push(serde_json::json!({
                "address": address,
                "device_type": device_type,
                "owner": gateway.owner,
                "owner_changed_at": gateway.owner_changed_at,
                "created_at": gateway.created_at,
                "updated_at": gateway.updated_at,
                "num_location_asserts": gateway.num_location_asserts,
                "metadata": gateway.metadata.map(|md| {
                    serde_json::json!({
                        "location_info": md.location_info.map(|li| {
                            serde_json::json!({
                                "location": li.location,
                                "location_changed_at": li.location_changed_at,
                            })
                        }),
                        "deployment_info": md.deployment_info.map(|di| {
                            serde_json::json!({
                                "antenna": di.antenna,
                                "elevation": di.elevation,
                                "azimuth": di.azimuth,
                            })
                        }),
                    })
                }),
            }));
        }
    }

    let output = serde_json::json!({
        "total": gateways.len(),
        "gateways": gateways
    });

    Msg::ok(serde_json::to_string_pretty(&output)?)
}
