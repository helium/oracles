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
pub mod service_provider;
mod settings;
pub mod sp_boosted_rewards_bans;
pub mod speedtests;
pub mod speedtests_average;
pub mod subscriber_location;
pub mod subscriber_verified_mapping_event;
pub mod telemetry;
pub mod unique_connections;

pub use settings::Settings;

use async_trait::async_trait;
use std::error::Error;

pub enum GatewayResolution {
    GatewayNotFound,
    GatewayNotAsserted,
    AssertedLocation(u64),
    DataOnly,
}

impl GatewayResolution {
    pub fn is_not_found(&self) -> bool {
        matches!(self, GatewayResolution::GatewayNotFound)
    }
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
        use mobile_config::gateway_info::{DeviceType, GatewayInfo};

        match self.resolve_gateway_info(address).await? {
            None => Ok(GatewayResolution::GatewayNotFound),
            Some(GatewayInfo {
                device_type: DeviceType::WifiDataOnly,
                ..
            }) => Ok(GatewayResolution::DataOnly),
            Some(GatewayInfo {
                metadata: Some(metadata),
                ..
            }) => Ok(GatewayResolution::AssertedLocation(metadata.location)),
            Some(_) => Ok(GatewayResolution::GatewayNotAsserted),
        }
    }
}
