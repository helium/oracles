use helium_proto::services::{mobile_config, Channel, Endpoint};
use humantime_serde::re::humantime;
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
    pub fn connect_gateway_client(&self) -> mobile_config::GatewayClient<Channel> {
        let channel = connect_channel(self);
        mobile_config::GatewayClient::new(channel)
    }

    pub fn connect_authorization_client(&self) -> mobile_config::AuthorizationClient<Channel> {
        let channel = connect_channel(self);
        mobile_config::AuthorizationClient::new(channel)
    }

    pub fn connect_entity_client(&self) -> mobile_config::EntityClient<Channel> {
        let channel = connect_channel(self);
        mobile_config::EntityClient::new(channel)
    }

    pub fn connect_carrier_service_client(&self) -> mobile_config::CarrierServiceClient<Channel> {
        let channel = connect_channel(self);
        mobile_config::CarrierServiceClient::new(channel)
    }

    pub fn connect_hex_boosting_service_client(&self) -> mobile_config::HexBoostingClient<Channel> {
        let channel = connect_channel(self);
        mobile_config::HexBoostingClient::new(channel)
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
}

fn connect_channel(settings: &Settings) -> Channel {
    Endpoint::from(settings.url.clone())
        .connect_timeout(settings.connect_timeout)
        .timeout(settings.rpc_timeout)
        .connect_lazy()
}
