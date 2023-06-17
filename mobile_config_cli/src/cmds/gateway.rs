use super::{GetHotspot, PathBufKeypair};
use crate::{client, Msg, PrettyJson, Result};
use angry_purple_tiger::AnimalName;
use helium_crypto::PublicKey;
use helium_proto::services::mobile_config::{
    GatewayInfo as GatewayInfoProto, GatewayMetadata as GatewayMetadataProto,
};
use serde::Serialize;
use std::str::FromStr;

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

#[derive(Debug, Serialize)]
pub struct GatewayInfo {
    name: String,
    pubkey: PublicKey,
    metadata: Option<GatewayMetadata>,
}

#[derive(Debug, Serialize)]
pub struct GatewayMetadata {
    location: String,
    lat: f64,
    lon: f64,
}

impl TryFrom<GatewayInfoProto> for GatewayInfo {
    type Error = anyhow::Error;

    fn try_from(info: GatewayInfoProto) -> Result<Self, Self::Error> {
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
