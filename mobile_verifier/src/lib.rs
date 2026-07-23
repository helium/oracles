extern crate tls_init;

pub mod banning;
pub mod cell_type;
pub mod cli;
pub mod data_session;
pub mod geofence;
pub mod heartbeats;
pub mod iceberg;
pub mod reward_shares;
pub mod rewarder;
mod settings;
pub mod speedtests;
pub mod speedtests_average;
pub mod telemetry;
pub mod unique_connections;

pub use settings::Settings;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mobile_config::client::ClientError;
use mobile_config::gateway::service::info::DeviceType;
use rust_decimal::Decimal;
use solana::SolPubkey;

pub enum GatewayResolution {
    GatewayNotFound,
    GatewayNotAsserted,
    AssertedLocation(u64, DeviceType),
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
        use mobile_config::gateway::service::info::GatewayInfo;

        match self
            .resolve_gateway_info(address, gateway_query_timestamp)
            .await?
        {
            None => Ok(GatewayResolution::GatewayNotFound),
            Some(info) if info.is_data_only() => Ok(GatewayResolution::DataOnly),
            Some(GatewayInfo {
                metadata: Some(metadata),
                device_type,
                ..
            }) => Ok(GatewayResolution::AssertedLocation(
                metadata.location,
                device_type,
            )),
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
    /// HNT price as USD/HNT × 10^decimals — stamped on `GatewayReward.price` and
    /// the reward manifest.
    pub price_in_bones: u64,
    /// USD per bone (1 HNT = 10^decimals bones) — used only for reward telemetry
    /// (demand, price-per-GB); the payout rate itself is price-independent.
    pub price_per_bone: Decimal,
}

impl PriceInfo {
    pub fn new(price_in_bones: u64, decimals: u8) -> Self {
        // price_in_bones is USD/HNT × 10^decimals: ÷10^decimals gives USD/HNT,
        // and ÷10^decimals again gives USD per bone.
        let scale = Decimal::from(10_u64.pow(decimals as u32));
        let price_per_bone = Decimal::from(price_in_bones) / scale / scale;
        Self {
            price_in_bones,
            price_per_bone,
        }
    }
}

pub fn resolve_subdao_pubkey() -> SolPubkey {
    solana::SubDao::Mobile.key()
}

pub fn resolve_dao_pubkey() -> SolPubkey {
    solana::Dao::Hnt.key()
}

pub trait ToProtoDecimal {
    fn proto_decimal(&self) -> helium_proto::Decimal;
}

impl ToProtoDecimal for rust_decimal::Decimal {
    fn proto_decimal(&self) -> helium_proto::Decimal {
        helium_proto::Decimal {
            value: self.to_string(),
        }
    }
}

pub trait FromProtoDecimal {
    fn to_f64(&self) -> f64;
}

impl FromProtoDecimal for Option<helium_proto::Decimal> {
    fn to_f64(&self) -> f64 {
        let Some(dec) = self else {
            return 0.0;
        };

        dec.value.parse().ok().unwrap_or_default()
    }
}

#[cfg(test)]
tls_init::include_tls_tests!();

#[cfg(test)]
mod address_tests {
    use super::{resolve_dao_pubkey, resolve_subdao_pubkey};

    // On-chain addresses observed in the prod Solana indexer (dao_epoch_infos /
    // sub_dao_epoch_infos). These are the exact values the reward-price query
    // filters on, so pin the derived PDAs against them — a canary for a
    // helium-lib change or a wrong DAO/sub-DAO mapping.
    const HNT_DAO: &str = "BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie";
    const MOBILE_SUB_DAO: &str = "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22";

    #[test]
    fn resolvers_match_onchain_addresses() {
        assert_eq!(resolve_dao_pubkey().to_string(), HNT_DAO);
        assert_eq!(resolve_subdao_pubkey().to_string(), MOBILE_SUB_DAO);
    }
}
