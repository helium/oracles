use helium_crypto::PublicKeyBinary;
use helium_proto::Region as ProtoRegion;
use iot_config::client::{Gateways, RegionParamsInfo};
use retainer::Cache;
use std::{sync::Arc, time::Duration};

/// how often to evict expired items from the cache ( every 5 mins)
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 5);

#[derive(Clone)]
pub struct RegionCache<G> {
    pub gateways: G,
    pub cache: Arc<Cache<ProtoRegion, RegionParamsInfo>>,
    refresh_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum RegionCacheError<GatewayApiError> {
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
    #[error("region not found: {0}")]
    RegionNotFound(ProtoRegion),
    #[error("error querying gateway api")]
    GatewayApiError(GatewayApiError),
}

impl<G> RegionCache<G>
where
    G: Gateways,
{
    pub fn new(
        refresh_interval: Duration,
        gateways: G,
    ) -> Result<Self, RegionCacheError<G::Error>> {
        let cache = Arc::new(Cache::<ProtoRegion, RegionParamsInfo>::new());
        let clone = cache.clone();
        // monitor cache to handle evictions
        tokio::spawn(async move { clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });
        Ok(Self {
            gateways,
            cache,
            refresh_interval,
        })
    }

    pub async fn resolve_region_info(
        &self,
        region: ProtoRegion,
    ) -> Result<RegionParamsInfo, RegionCacheError<G::Error>> {
        match self.cache.get(&region).await {
            Some(hit) => {
                metrics::counter!("oracles_iot_verifier_region_params_cache_hit").increment(1);
                Ok(hit.value().clone())
            }
            _ => match self.gateways.clone().resolve_region_params(region).await {
                Ok(res) => {
                    metrics::counter!("oracles_iot_verifier_region_params_cache_miss").increment(1);
                    self.cache
                        .insert(region, res.clone(), self.refresh_interval)
                        .await;
                    Ok(res)
                }
                Err(err) => Err(RegionCacheError::GatewayApiError(err)),
            },
        }
    }
}
