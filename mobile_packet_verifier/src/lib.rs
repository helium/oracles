use std::sync::Arc;

use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::{
    self, authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
};

pub mod accumulate;
pub mod burner;
pub mod daemon;
pub mod event_ids;
pub mod pending_burns;
pub mod settings;

pub struct MobileConfigClients {
    gateway_client: Arc<client::GatewayClient>,
    auth_client: Arc<client::AuthorizationClient>,
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
