use crate::Result;
use async_trait::async_trait;
use helium_crypto::PublicKey;
use node_follower::gateway_resp::FollowerGatewayResp;

#[async_trait]
pub trait GatewayInfoResolver {
    async fn resolve_gateway_info(&mut self, address: &PublicKey) -> Result<FollowerGatewayResp>;
}
