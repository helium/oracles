use crate::Settings;
use helium_crypto::PublicKeyBinary;
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

/// how long each cached items takes to expire ( 12 hours in seconds)
const CACHE_TTL: Duration = Duration::from_secs(12 * (60 * 60));
/// how often to evict expired items from the cache ( every 1 hour)
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

pub struct GatewayCache {
    pub follower_service: FollowerService,
    pub cache: Arc<Cache<PublicKeyBinary, GatewayInfo>>,
    pub cache_monitor: JoinHandle<()>,
}

#[derive(thiserror::Error, Debug)]
#[error("gateway not found: {0}")]
pub struct GatewayNotFound(PublicKeyBinary);

impl GatewayCache {
    pub fn from_settings(settings: &Settings) -> Self {
        let follower_service = FollowerService::from_settings(&settings.follower);
        let cache = Arc::new(Cache::<PublicKeyBinary, GatewayInfo>::new());
        let clone = cache.clone();
        // monitor cache to handle evictions
        let cache_monitor =
            tokio::spawn(async move { clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });
        Self {
            follower_service,
            cache,
            cache_monitor,
        }
    }

    pub async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, GatewayNotFound> {
        match self.cache.get(address).await {
            Some(hit) => {
                metrics::increment_counter!("oracles_iot_verifier_gateway_cache_hit");
                Ok(hit.value().clone())
            }
            _ => {
                match self
                    .follower_service
                    .clone()
                    .resolve_gateway_info(address)
                    .await
                {
                    Ok(res) => {
                        tracing::debug!("cache miss: {:?}", address);
                        metrics::increment_counter!("oracles_iot_verifier_gateway_cache_miss");
                        self.cache
                            .insert(address.clone(), res.clone(), CACHE_TTL)
                            .await;
                        Ok(res)
                    }
                    _ => Err(GatewayNotFound(address.clone())),
                }
            }
        }
    }
}
