use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{
    self, authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
};

pub mod accumulate;
pub mod banning;
pub mod burner;
pub mod daemon;
pub mod event_ids;
pub mod pending_burns;
pub mod pending_txns;
pub mod settings;

const BYTES_PER_DC: u64 = 20_000;

pub fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    bytes.div_ceil(BYTES_PER_DC)
}

pub fn dc_to_bytes(dcs: u64) -> u64 {
    dcs * BYTES_PER_DC
}

pub struct MobileConfigClients {
    gateway_client: client::GatewayClient,
    auth_client: client::AuthorizationClient,
}

impl MobileConfigClients {
    pub fn new(settings: &client::Settings) -> anyhow::Result<Self> {
        Ok(Self {
            gateway_client: client::GatewayClient::from_settings(settings)?,
            auth_client: client::AuthorizationClient::from_settings(settings)?,
        })
    }
}

#[async_trait::async_trait]
pub trait MobileConfigResolverExt {
    async fn is_gateway_known(&self, public_key: &PublicKeyBinary) -> bool;
    async fn is_routing_key_known(&self, public_key: &PublicKeyBinary) -> bool;
}

#[async_trait::async_trait]
impl MobileConfigResolverExt for MobileConfigClients {
    async fn is_gateway_known(&self, public_key: &PublicKeyBinary) -> bool {
        match self.gateway_client.resolve_gateway_info(public_key).await {
            Ok(res) => res.is_some(),
            Err(_err) => false,
        }
    }

    async fn is_routing_key_known(&self, public_key: &PublicKeyBinary) -> bool {
        self.auth_client
            .verify_authorized_key(public_key, NetworkKeyRole::MobileRouter)
            .await
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_dc() {
        assert_eq!(1, bytes_to_dc(1));
        assert_eq!(1, bytes_to_dc(20_000));
        assert_eq!(2, bytes_to_dc(20_001));
    }
}
