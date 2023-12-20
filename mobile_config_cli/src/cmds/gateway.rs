use super::{GetHotspot, GetHotspotBatch, PathBufKeypair};
use crate::{client, Msg, PrettyJson, Result};
use angry_purple_tiger::AnimalName;
use futures::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::services::mobile_config::{
    GatewayInfo as GatewayInfoProto, GatewayMetadata as GatewayMetadataProto,
};
use mobile_config::gateway_info::DeviceType;
use serde::Serialize;
use std::str::FromStr;

pub type GatewayInfoStream = futures::stream::BoxStream<'static, GatewayInfo>;

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
        })
    }
}
