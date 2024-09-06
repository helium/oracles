use crate::{client::DenyListClient, models::metadata::Asset, Error, Result, Settings};
use helium_crypto::{PublicKey, PublicKeyBinary};
use serde::Serialize;
use std::{fs, path};
use xorf_generator::{edge_hash, public_key_hash, Filter};

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
    pub filter: Option<Filter>,
    pub sign_keys: Vec<PublicKey>,
}

impl TryFrom<Vec<PublicKeyBinary>> for DenyList {
    type Error = Error;
    fn try_from(v: Vec<PublicKeyBinary>) -> Result<Self> {
        let keys: Vec<u64> = v.iter().map(public_key_hash).collect();
        let filter = Filter::new(0, xorf_generator::xorf::Xor32::from(&keys))
            .map_err(|_| Error::invalid_filter("filter"))?;
        let client = DenyListClient::new()?;
        Ok(Self {
            tag_name: 0,
            client,
            filter: Some(filter),
            sign_keys: vec![],
        })
    }
}

impl TryFrom<Vec<(PublicKeyBinary, PublicKeyBinary)>> for DenyList {
    type Error = Error;
    fn try_from(v: Vec<(PublicKeyBinary, PublicKeyBinary)>) -> Result<Self> {
        let keys: Vec<u64> = v.iter().map(|e| edge_hash(&e.0, &e.1)).collect();
        let filter = Filter::new(0, xorf_generator::xorf::Xor32::from(&keys))
            .map_err(|_| Error::invalid_filter("filter"))?;
        let client = DenyListClient::new()?;
        Ok(Self {
            tag_name: 0,
            client,
            filter: Some(filter),
            sign_keys: vec![],
        })
    }
}

impl DenyList {
    pub fn new(settings: &Settings) -> Result<Self> {
        tracing::debug!("initializing new denylist");
        // if exists default to the local saved filter bin, otherwise default to
        // empty filter a local filter should always be present after the
        // verifier has been run at least once in the current dir and has
        // previously successfully downloaded a filter from github
        let sign_keys = settings.sign_keys()?;
        let filter = fs::read(FILTER_BIN_PATH)
            .map_err(Error::from)
            .and_then(|bytes| filter_from_bin(&bytes, &sign_keys))
            .map(Some)
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "failed to initialise with a denylist filter, filter is currently empty"
                );
                None
            });

        let client = DenyListClient::new()?;
        Ok(Self {
            // default tag to 0, proper tag name will be set on first call to
            // update_to_latest
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
                    self.filter = Some(filter);
                    self.tag_name = new_tag_name;
                    save_local_filter_bin(&bin, FILTER_BIN_PATH)?;
                }
            }
        }
        Ok(())
    }

    pub fn contains_key(&self, key: &PublicKeyBinary) -> bool {
        if let Some(filter) = &self.filter {
            filter.contains(key)
        } else {
            tracing::warn!("empty denylist filter, rejecting key");
            true
        }
    }

    pub fn contains_edge(&self, beaconer: &PublicKeyBinary, witness: &PublicKeyBinary) -> bool {
        if let Some(filter) = &self.filter {
            filter.contains_edge(beaconer, witness)
        } else {
            tracing::warn!("empty denylist filter, rejecting edge");
            true
        }
    }
}

/// deconstruct bytes into the filter component parts
pub fn filter_from_bin(bin: &[u8], sign_keys: &[PublicKey]) -> Result<Filter> {
    let filter = Filter::from_bytes(bin).map_err(|_| Error::invalid_filter("filter"))?;
    let verified = sign_keys.iter().any(|pubkey| {
        filter
            .verify(pubkey)
            .inspect(|_res| {
                tracing::info!(%pubkey, "valid denylist signer");
            })
            .is_ok()
    });
    if !verified {
        tracing::warn!("filter signature verification failed");
        return Err(Error::invalid_filter("signature verification"));
    }
    Ok(filter)
}

/// save a copy of the xor file locally
// the local copy will be used should during init github be unreachable
pub fn save_local_filter_bin(bin: &Vec<u8>, path: &str) -> Result {
    if let Some(parent) = path::PathBuf::from(path).parent() {
        fs::create_dir_all(parent)?;
        fs::write(path, bin)?;
    }
    Ok(())
}
