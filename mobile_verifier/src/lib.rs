#![deny(unsafe_code)]

extern crate tls_init;

pub mod banning;
pub mod boosting_oracles;
pub mod cell_type;
pub mod cli;
pub mod coverage;
pub mod data_session;
pub mod geofence;
pub mod heartbeats;
pub mod reward_shares;
pub mod rewarder;
pub mod seniority;
mod settings;
pub mod speedtests;
pub mod speedtests_average;
pub mod telemetry;
pub mod unique_connections;

pub use settings::Settings;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mobile_config::client::ClientError;
use rust_decimal::Decimal;
use solana::SolPubkey;

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
    async fn resolve_gateway(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        gateway_query_timestamp: &DateTime<Utc>,
    ) -> Result<GatewayResolution, ClientError>;
}

#[async_trait]
impl GatewayResolver for mobile_config::GatewayClient {
    async fn resolve_gateway(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        gateway_query_timestamp: &DateTime<Utc>,
    ) -> Result<GatewayResolution, ClientError> {
        use mobile_config::gateway::client::GatewayInfoResolver;
        use mobile_config::gateway::service::info::{DeviceType, GatewayInfo};

        match self
            .resolve_gateway_info(address, gateway_query_timestamp)
            .await?
        {
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
    async fn is_authorized(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        role: helium_proto::services::mobile_config::NetworkKeyRole,
    ) -> Result<bool, ClientError>;
}

#[async_trait]
impl IsAuthorized for mobile_config::client::AuthorizationClient {
    async fn is_authorized(
        &self,
        address: &helium_crypto::PublicKeyBinary,
        role: helium_proto::services::mobile_config::NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        use mobile_config::client::authorization_client::AuthorizationVerifier;
        self.verify_authorized_key(address, role).await
    }
}

#[derive(Clone, Debug)]
pub struct PriceInfo {
    pub price_in_bones: u64,
    pub price_per_token: Decimal,
    pub price_per_bone: Decimal,
    pub decimals: u8,
}

impl PriceInfo {
    pub fn new(price_in_bones: u64, decimals: u8) -> Self {
        let price_per_token =
            Decimal::from(price_in_bones) / Decimal::from(10_u64.pow(decimals as u32));
        let price_per_bone = price_per_token / Decimal::from(10_u64.pow(decimals as u32));
        Self {
            price_in_bones,
            price_per_token,
            price_per_bone,
            decimals,
        }
    }
}

pub fn resolve_subdao_pubkey() -> SolPubkey {
    solana::SubDao::Mobile.key()
}

#[cfg(test)]
tls_init::include_tls_tests!();
