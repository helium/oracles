pub mod cell_type;
pub mod cli;
pub mod coverage;
mod data_session;
pub mod heartbeats;
mod reward_shares;
pub mod rewarder;
mod settings;
mod speedtests;
mod speedtests_average;
mod subscriber_location;
mod telemetry;

pub use settings::Settings;

use async_trait::async_trait;

use mobile_config::gateway_info::GatewayMetadata;
pub enum GatewayResolution {
    GatewayNotFound,
    GatewayNotAsserted,
    GatewayAsserted(GatewayMetadata),
}

#[async_trait]
pub trait GatewayResolver {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn resolve_gateway(
        &self,
        address: &helium_crypto::PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error>;
}

#[async_trait]
impl GatewayResolver for mobile_config::GatewayClient {
    type Error = mobile_config::client::ClientError;

    async fn resolve_gateway(
        &self,
        address: &helium_crypto::PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error> {
        use mobile_config::gateway_info::{GatewayInfo, GatewayInfoResolver};
        match self.resolve_gateway_info(address).await? {
            None => Ok(GatewayResolution::GatewayNotFound),
            Some(GatewayInfo {
                metadata: Some(metadata),
                ..
            }) => Ok(GatewayResolution::GatewayAsserted(metadata)),
            Some(_) => Ok(GatewayResolution::GatewayNotAsserted),
        }
    }
}

#[async_trait]
pub trait IsAuthorized {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn is_authorized(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        role: helium_proto::services::mobile_config::NetworkKeyRole,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
impl IsAuthorized for mobile_config::client::AuthorizationClient {
    type Error = mobile_config::client::ClientError;

    async fn is_authorized(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        role: helium_proto::services::mobile_config::NetworkKeyRole,
    ) -> Result<bool, Self::Error> {
        self.verify_authorized_key(address, role).await
    }
}
