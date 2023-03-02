use helium_proto::services::{iot_config, Channel, Endpoint};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// signing key used to sign requests to iot config service
    pub keypair: String,
    /// grpc to iot config service
    #[serde(with = "http_serde::uri", default = "default_url")]
    pub url: http::Uri,
    /// Connect timeout for follower in seconds. Default 5
    #[serde(default = "default_connect_timeout")]
    pub connect: u64,
    /// RPC timeout for follower in seconds. Default 5
    #[serde(default = "default_rpc_timeout")]
    pub rpc: u64,
    /// batch size for gateway stream results. Default 100
    #[serde(default = "default_batch_size")]
    pub batch: u32,
}

pub fn default_url() -> http::Uri {
    http::Uri::from_static("http://127.0.0.1:8080")
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

impl Settings {
    pub fn connect_endpoint(&self) -> Channel {
        Endpoint::from(self.url.clone())
            .connect_timeout(Duration::from_secs(self.connect))
            .timeout(Duration::from_secs(self.rpc))
            .connect_lazy()
    }

    pub fn connect_iot_gateway_config(
        &self,
        channel: Channel,
    ) -> iot_config::GatewayClient<Channel> {
        iot_config::GatewayClient::new(channel)
    }
    pub fn connect_iot_region_config(
        &self,
        channel: Channel,
    ) -> iot_config::admin_client::AdminClient<Channel> {
        iot_config::admin_client::AdminClient::new(channel)
    }
    pub fn keypair(&self) -> Result<helium_crypto::Keypair, Box<helium_crypto::Error>> {
        let data = std::fs::read(&self.keypair).map_err(helium_crypto::Error::from)?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }
}
