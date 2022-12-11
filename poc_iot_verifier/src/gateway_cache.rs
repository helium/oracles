use crate::{Error, Result, Settings};
use helium_crypto::PublicKeyBinary;
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use retainer::Cache;
use std::time::Duration;

const CACHE_TTL: u64 = 86400;

pub struct GatewayCache {
    pub follower_service: FollowerService,
    pub cache: Cache<PublicKeyBinary, GatewayInfo>,
}

impl GatewayCache {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let follower_service = FollowerService::from_settings(&settings.follower)?;
        let cache = Cache::<PublicKeyBinary, GatewayInfo>::new();
        Ok(Self {
            follower_service,
            cache,
        })
    }

    pub async fn resolve_gateway_info(&self, address: &PublicKeyBinary) -> Result<GatewayInfo> {
        match self.cache.get(address).await {
            Some(hit) => {
                tracing::debug!("gateway cache hit: {:?}", address);
                metrics::increment_counter!("oracles_poc_iot_verifier_gateway_cache_hit");
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
                        metrics::increment_counter!("oracles_poc_iot_verifier_gateway_cache_miss");
                        self.cache
                            .insert(address.clone(), res.clone(), Duration::from_secs(CACHE_TTL))
                            .await;
                        Ok(res)
                    }
                    _ => Err(Error::GatewayNotFound(format!("{address:?}"))),
                }
            }
        }
    }
}
