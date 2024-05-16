use crate::gateway_updater::MessageReceiver;
use helium_crypto::PublicKeyBinary;
use iot_config::gateway_info::GatewayInfo;

#[derive(Clone)]
pub struct GatewayCache {
    gateway_cache_receiver: MessageReceiver,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayCacheError {
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
}

impl GatewayCache {
    pub fn new(gateway_cache_receiver: MessageReceiver) -> Self {
        Self {
            gateway_cache_receiver,
        }
    }

    pub async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, GatewayCacheError> {
        match self.gateway_cache_receiver.borrow().get(address) {
            Some(hit) => {
                metrics::counter!("oracles_iot_verifier_gateway_cache_hit").increment(1);
                Ok(hit.clone())
            }
            None => {
                metrics::counter!("oracles_iot_verifier_gateway_cache_miss").increment(1);
                Err(GatewayCacheError::GatewayNotFound(address.clone()))
            }
        }
    }
}
