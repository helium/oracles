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
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::error::Error;

pub const MOBILE_SUB_DAO_ONCHAIN_ADDRESS: &str = "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk";

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

pub struct PriceConverter;

impl PriceConverter {
    pub fn hnt_bones_to_pricer_format(hnt_bone_price: Decimal) -> u64 {
        (hnt_bone_price * dec!(1_0000_0000) * dec!(1_0000_0000))
            .to_u64()
            .unwrap_or_default()
    }

    // Hnt prices are supplied from pricer in 10^8
    pub fn pricer_format_to_hnt_bones(hnt_price: u64) -> Decimal {
        Decimal::from(hnt_price) / dec!(1_0000_0000) / dec!(1_0000_0000)
    }

    pub fn pricer_format_to_hnt(hnt_price: u64) -> Decimal {
        Decimal::from(hnt_price) / dec!(1_0000_0000)
    }
}
