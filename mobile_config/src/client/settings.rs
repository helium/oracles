use helium_crypto::{Keypair, PublicKey};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// grpc url to the mobile config oracle server
    #[serde(with = "http_serde::uri")]
    pub url: http::Uri,
    /// Base64 encoded string of the helium keypair
    #[serde(
        deserialize_with = "crate::deserialize_helium_keypair",
        skip_serializing
    )]
    pub signing_keypair: Arc<Keypair>,
    /// B58 encoded public key of the mobile config server for verification
    pub config_pubkey: PublicKey,
    /// Connect timeout for the mobile config client in seconds. Default 5
    #[serde(with = "humantime_serde", default = "default_connect_timeout")]
    pub connect_timeout: Duration,
    /// RPC timeout for mobile config client in seconds. Default 5
    #[serde(with = "humantime_serde", default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,
    /// Batch size for hotspot metadata stream results. Default 100
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Batch size for hex boosting stream results. Default 100
    #[serde(default = "default_hex_boosting_batch_size")]
    pub hex_boosting_batch_size: u32,
    #[serde(with = "humantime_serde", default = "default_cache_ttl_in_secs")]
    pub cache_ttl: Duration,
}

fn default_connect_timeout() -> Duration {
    humantime::parse_duration("5 seconds").unwrap()
}

fn default_rpc_timeout() -> Duration {
    humantime::parse_duration("5 seconds").unwrap()
}

fn default_batch_size() -> u32 {
    100
}

fn default_hex_boosting_batch_size() -> u32 {
    100
}

fn default_cache_ttl_in_secs() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

impl Settings {
    pub fn channel(&self) -> Result<Channel, tonic::transport::Error> {
        let endpoint = Endpoint::from(self.url.clone())
            .connect_timeout(self.connect_timeout)
            .timeout(self.rpc_timeout)
            .tls_config(ClientTlsConfig::new().with_enabled_roots())?;

        Ok(endpoint.connect_lazy())
    }
}
