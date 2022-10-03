use crate::denylist::client::DenyListClient;
use crate::{models::metadata::Asset, *};
use bytes::Buf;
use helium_crypto::PublicKey;
use serde::Serialize;
use std::hash::Hasher;
use twox_hash::XxHash64;
use xorf::{Filter as XorFilter, Xor32};

pub const SERIAL_SIZE: usize = 32;

#[derive(Serialize)]
pub struct DenyList {
    pub tag_name: u64,
    #[serde(skip_serializing)]
    pub filter: Xor32,
}

impl Default for DenyList {
    fn default() -> Self {
        Self::new()
    }
}

impl DenyList {
    pub fn new() -> Self {
        tracing::debug!("initializing new denylist");
        // default to an empty filter
        let filter = Xor32::from(Vec::new());
        Self {
            tag_name: 0,
            filter,
        }
    }

    pub async fn update_to_latest(&mut self, metadata_url: &String) {
        tracing::info!("checking for updated denylist");
        // get a new client to fetch denylist metadata
        let mut dl_client = match DenyListClient::new() {
            Ok(res) => res,
            Err(_) => {
                tracing::error!("failing to initialize denylist client");
                return;
            }
        };
        // get the metadata
        let metadata = match dl_client.get_metadata(metadata_url).await {
            Some(res) => res,
            None => {
                tracing::error!("failing to parse download denylist metadata");
                return;
            }
        };
        // parse tag name from metadata, if higher than our current
        // then then download the new bin
        let new_tag_name = match metadata.tag_name.parse::<u64>() {
            Ok(res) => res,
            Err(_) => {
                tracing::error!("failing to parse metadata tagname");
                return;
            }
        };
        if new_tag_name > self.tag_name {
            tracing::info!("updated denylist tag: {:?}", new_tag_name);
            // get the asset
            // filter out any assets which do not have a name == "filter.bin"
            let assets: Vec<Asset> = metadata
                .assets
                .into_iter()
                .filter(|a| a.name == "filter.bin")
                .collect();
            // we should be left with a single asset
            // at least this is the assumption the erlang implementation followed
            if let Some(asset) = assets.first() {
                tracing::info!("found asset for tag, updating filter");
                let asset_url = asset.browser_download_url.clone();
                let bin = match dl_client.get_bin(&asset_url).await {
                    Some(res) => res,
                    None => return,
                };
                // slice the binary into its component parts
                // TODO: verify sig
                let mut buf: &[u8] = &bin;
                let _version = buf.get_u8();
                let signature_len = buf.get_u16_le() as usize;
                let _signature = buf.copy_to_bytes(signature_len).to_vec();
                let _serial = buf.get_u32_le();
                let filter = bincode::deserialize::<Xor32>(buf).unwrap();
                self.filter = filter;
                self.tag_name = new_tag_name;
            }
        }
    }

    pub async fn check_key(&self, pub_key: &PublicKey) -> bool {
        self.filter.contains(&public_key_hash(pub_key))
    }
}

fn public_key_hash(public_key: &PublicKey) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(&public_key.to_vec());
    hasher.finish()
}
