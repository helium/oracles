pub mod boosting_oracles;
pub mod cell_type;
pub mod cli;
pub mod coverage;
pub mod data_session;
pub mod geofence;
pub mod heartbeats;
pub mod radio_threshold;
pub mod reward_shares;
pub mod rewarder;
pub mod seniority;
mod settings;
pub mod sp_boosted_rewards_bans;
pub mod speedtests;
pub mod speedtests_average;
pub mod subscriber_location;
pub mod subscriber_mapping_event;
pub mod telemetry;

pub use settings::Settings;

use async_trait::async_trait;
use std::error::Error;

pub enum GatewayResolution {
    GatewayNotFound,
    GatewayNotAsserted,
    AssertedLocation(u64),
}

#[async_trait::async_trait]
pub trait GatewayResolver: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

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
        use mobile_config::client::gateway_client::GatewayInfoResolver;
        use mobile_config::gateway_info::GatewayInfo;
        match self.resolve_gateway_info(address).await? {
            None => Ok(GatewayResolution::GatewayNotFound),
            Some(GatewayInfo {
                metadata: Some(metadata),
                ..
            }) => Ok(GatewayResolution::AssertedLocation(metadata.location)),
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
        use mobile_config::client::authorization_client::AuthorizationVerifier;
        self.verify_authorized_key(address, role).await
    }
}
