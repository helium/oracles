use crate::{client::DenyListClient, models::metadata::Asset, Error, Result, Settings};
use bytes::Buf;
use helium_crypto::{PublicKey, PublicKeyBinary, Verify};
use serde::Serialize;
use std::{fs, hash::Hasher, path};
use twox_hash::XxHash64;
use xorf::{Filter as XorFilter, Xor32};

pub const SERIAL_SIZE: usize = 32;
/// a copy of the last saved filter bin downloaded from github
/// if present will be used to initialise the denylist upon verifier startup
// TODO: look at using the tempfile crate to handle this
const FILTER_BIN_PATH: &str = "./tmp/last_saved_filter.bin";

#[derive(Serialize)]
pub struct DenyList {
    pub tag_name: u64,
    #[serde(skip_serializing)]
    pub client: DenyListClient,
    #[serde(skip_serializing)]
    pub filter: Xor32,
    pub sign_keys: Vec<PublicKey>,
}

impl TryFrom<Vec<PublicKeyBinary>> for DenyList {
    type Error = Error;
    fn try_from(v: Vec<PublicKeyBinary>) -> Result<Self> {
        let keys: Vec<u64> = v.into_iter().map(public_key_hash).collect();
        let filter = Xor32::from(&keys);
        let client = DenyListClient::new()?;
        Ok(Self {
            tag_name: 0,
            client,
            filter,
            sign_keys: vec![],
        })
    }
}

impl TryFrom<Vec<(PublicKeyBinary, PublicKeyBinary)>> for DenyList {
    type Error = Error;
    fn try_from(v: Vec<(PublicKeyBinary, PublicKeyBinary)>) -> Result<Self> {
        let keys: Vec<u64> = v
            .into_iter()
            .map(|e| edge_hash((e.0.as_ref(), e.1.as_ref())))
            .collect();
        let filter = Xor32::from(&keys);
        let client = DenyListClient::new()?;
        Ok(Self {
            tag_name: 0,
            client,
            filter,
            sign_keys: vec![],
        })
    }
}

impl DenyList {
    pub fn new(settings: &Settings) -> Result<Self> {
        tracing::debug!("initializing new denylist");
        // if exists default to the local saved filter bin,
        // otherwise default to empty filter
        // a local filter should always be present
        // after the verifier has been run at least once
        // in the current dir and has previously successfully downloaded
        // a filter from github
        let bin: Vec<u8> = fs::read(FILTER_BIN_PATH).unwrap_or_else(|_| {
            tracing::warn!(
                "failed to initialise with a denylist filter, filter is currently empty"
            );
            Vec::new()
        });
        let sign_keys = settings.sign_keys()?;
        let filter = filter_from_bin(&bin, &sign_keys).unwrap_or_else(|_| Xor32::from(Vec::new()));
        let client = DenyListClient::new()?;
        Ok(Self {
            // default tag to 0, proper tag name will be set on first
            // call to update_to_latest
            tag_name: 0,
            client,
            filter,
            sign_keys,
        })
    }

    pub async fn update_to_latest(&mut self, metadata_url: &String) -> Result {
        tracing::info!("checking for updated denylist, url: {metadata_url} ");

        let metadata = self.client.get_metadata(metadata_url).await?;
        let new_tag_name = metadata.tag_name.parse::<u64>()?;
        tracing::info!(
            "local denylist tag: {:?}, remote denylist tag: {:?}",
            self.tag_name,
            new_tag_name
        );
        if new_tag_name > self.tag_name {
            tracing::info!(
                "remote tag is newer, updating denylist to {:?}",
                new_tag_name
            );
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
                tracing::debug!("found asset for tag");
                let asset_url = &asset.browser_download_url;
                let bin = self.client.get_bin(asset_url).await?;
                if let Ok(filter) = filter_from_bin(&bin, &self.sign_keys) {
                    self.filter = filter;
                    self.tag_name = new_tag_name;
                    save_local_filter_bin(&bin, FILTER_BIN_PATH)?;
                }
            }
        }
        Ok(())
    }

    pub fn check_key<K: AsRef<[u8]>>(&self, pub_key: K) -> bool {
        if self.filter.len() == 0 {
            tracing::warn!("empty denylist filter, rejecting key");
            return true;
        }
        self.filter.contains(&public_key_hash(pub_key))
    }

    pub fn check_edge(&self, beaconer: &[u8], witness: &[u8]) -> bool {
        if self.filter.len() == 0 {
            tracing::warn!("empty denylist filter, rejecting edge");
            return true;
        }
        // sort both keys into lexiographic order, so edges are not considered directional
        let mut a = [beaconer, witness];
        a.sort();
        self.filter.contains(&edge_hash((a[0], a[1])))
    }
}

/// deconstruct bytes into the filter component parts
pub fn filter_from_bin(bin: &Vec<u8>, sign_keys: &[PublicKey]) -> Result<Xor32> {
    if bin.is_empty() {
        return Err(Error::InvalidBinary("invalid filter bin".to_string()));
    }
    let mut buf: &[u8] = bin;
    // whilst we dont use version, we do need to advance the cursor
    let _version = buf.get_u8();
    let signature_len = buf.get_u16_le() as usize;
    let signature = buf.copy_to_bytes(signature_len).to_vec();
    sign_keys
        .iter()
        .any(|pubkey| {
            pubkey
                .verify(buf, &signature)
                .map(|res| {
                    tracing::info!(%pubkey, "valid denylist signer");
                    res
                })
                .is_ok()
        })
        .then(|| {
            buf.advance(4);
            bincode::deserialize::<Xor32>(buf)
        })
        .transpose()?
        .ok_or_else(|| {
            tracing::warn!("filter signature verification failed");
            Error::InvalidBinary("filter signature verification failed".to_string())
        })
}

fn public_key_hash<R: AsRef<[u8]>>(public_key: R) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(public_key.as_ref());
    hasher.finish()
}

fn edge_hash(edge_key: (&[u8], &[u8])) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(edge_key.0);
    hasher.write(edge_key.1);
    hasher.finish()
}

/// save a copy of the xor file locally
// the local copy will be used should during init
// github be unreachable
pub fn save_local_filter_bin(bin: &Vec<u8>, path: &str) -> Result {
    if let Some(parent) = path::PathBuf::from(path).parent() {
        fs::create_dir_all(parent)?;
        fs::write(path, bin)?;
    }
    Ok(())
}
