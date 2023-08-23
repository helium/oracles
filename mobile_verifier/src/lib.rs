mod cell_type;
pub mod coverage;
mod data_session;
pub mod heartbeats;
pub mod reward_shares;
mod settings;
pub mod speedtests;
mod subscriber_location;
mod telemetry;

pub mod cli;
pub mod rewarder;

pub use settings::Settings;

use async_trait::async_trait;

#[async_trait]
pub trait HasOwner {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn has_owner(
        &self,
        address: &helium_crypto::PublicKeyBinary,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
impl HasOwner for mobile_config::GatewayClient {
    type Error = mobile_config::client::ClientError;

    async fn has_owner(
        &self,
        address: &helium_crypto::PublicKeyBinary,
    ) -> Result<bool, Self::Error> {
        use mobile_config::gateway_info::GatewayInfoResolver;
        Ok(self.resolve_gateway_info(address).await?.is_some())
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
