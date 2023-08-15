pub mod cell_type;
pub mod cli;
pub mod data_session;
pub mod heartbeats;
pub mod reward_shares;
pub mod rewarder;
pub mod settings;
pub mod speedtests;
pub mod subscriber_location;
mod telemetry;

pub use settings::Settings;

#[async_trait::async_trait]
pub trait HasOwner {
    type Error;

    async fn has_owner(
        &self,
        address: &helium_crypto::PublicKeyBinary,
    ) -> Result<bool, Self::Error>;
}

#[async_trait::async_trait]
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
