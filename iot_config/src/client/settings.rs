use serde::Deserialize;
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    /// grpc url to the iot config oracle server
    #[serde(with = "http_serde::uri")]
    pub url: http::Uri,
    /// File from which to load keypair for signing config client requests
    pub signing_keypair: String,
    /// B58 encoded public key of the iot config server for verifying responses
    pub config_pubkey: String,
    /// Connect timeout for the iot config client in seconds. Default 5
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    /// RPC timeout for iot config client in seconds. Default 5
    #[serde(default = "default_rpc_timeout")]
    pub rpc_timeout: u64,
    /// Batch size for gateway info stream results. Default 1000
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_connect_timeout() -> u64 {
    5
}

fn default_rpc_timeout() -> u64 {
    5
}

fn default_batch_size() -> u32 {
    1000
}

impl Settings {
    pub fn signing_keypair(
        &self,
    ) -> Result<Arc<helium_crypto::Keypair>, Box<helium_crypto::Error>> {
        let data = std::fs::read(&self.signing_keypair).map_err(helium_crypto::Error::from)?;
        Ok(Arc::new(helium_crypto::Keypair::try_from(&data[..])?))
    }

    pub fn config_pubkey(&self) -> Result<helium_crypto::PublicKey, helium_crypto::Error> {
        helium_crypto::PublicKey::from_str(&self.config_pubkey)
    }
}
