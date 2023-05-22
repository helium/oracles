use crate::gateway_updater::MessageReceiver;
use helium_crypto::PublicKeyBinary;
use iot_config::gateway_info::GatewayInfo;

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
                metrics::increment_counter!("oracles_iot_verifier_gateway_cache_hit");
                Ok(hit.clone())
            }
            None => {
                metrics::increment_counter!("oracles_iot_verifier_gateway_cache_miss");
                Err(GatewayCacheError::GatewayNotFound(address.clone()))
            }
        }
    }
}
