use helium_proto::services::{mobile_config, Channel, Endpoint};
use serde::Deserialize;
use std::{str::FromStr, sync::Arc, time::Duration};

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    /// grpc url to the mobile config oracle server
    #[serde(with = "http_serde::uri")]
    pub url: http::Uri,
    /// File from which to load config server signing keypair
    pub signing_keypair: String,
    /// B58 encoded public key of the mobile config server for verification
    pub config_pubkey: String,
    /// Connect timeout for the mobile config client in seconds. Default 5
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    /// RPC timeout for mobile config client in seconds. Default 5
    #[serde(default = "default_rpc_timeout")]
    pub rpc_timeout: u64,
    /// Batch size for hotspot metadata stream results. Default 100
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_cache_ttl_in_secs")]
    pub cache_ttl_in_secs: u64,
}

pub fn default_connect_timeout() -> u64 {
    5
}

pub fn default_rpc_timeout() -> u64 {
    5
}

pub fn default_batch_size() -> u32 {
    100
}

pub fn default_cache_ttl_in_secs() -> u64 {
    60 * 60
}

impl Settings {
    pub fn connect(&self) -> mobile_config::GatewayClient<Channel> {
        let channel = Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect_timeout))
            .timeout(Duration::from_secs(self.rpc_timeout))
            .connect_lazy();
        mobile_config::GatewayClient::new(channel)
    }

    pub fn signing_keypair(
        &self,
    ) -> Result<Arc<helium_crypto::Keypair>, Box<helium_crypto::Error>> {
        let data = std::fs::read(&self.signing_keypair).map_err(helium_crypto::Error::from)?;
        Ok(Arc::new(helium_crypto::Keypair::try_from(&data[..])?))
    }

    pub fn config_pubkey(&self) -> Result<helium_crypto::PublicKey, helium_crypto::Error> {
        helium_crypto::PublicKey::from_str(&self.config_pubkey)
    }

    pub fn cache_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.cache_ttl_in_secs)
    }
}
