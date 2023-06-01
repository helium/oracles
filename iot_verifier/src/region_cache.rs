use crate::Settings;
use helium_crypto::PublicKeyBinary;
use helium_proto::Region as ProtoRegion;
use iot_config::client::{
    Client as IotConfigClient, ClientError as IotConfigClientError, RegionParamsInfo,
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};

/// how often to evict expired items from the cache ( every 5 mins)
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 5);

pub struct RegionCache {
    pub iot_config_client: IotConfigClient,
    pub cache: Arc<Cache<ProtoRegion, RegionParamsInfo>>,
    refresh_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum RegionCacheError {
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
    #[error("region not found: {0}")]
    RegionNotFound(ProtoRegion),
    #[error("error querying iot config service")]
    IotConfigClient(#[from] IotConfigClientError),
}

impl RegionCache {
    pub fn from_settings(
        settings: &Settings,
        iot_config_client: IotConfigClient,
    ) -> Result<Self, RegionCacheError> {
        let cache = Arc::new(Cache::<ProtoRegion, RegionParamsInfo>::new());
        let clone = cache.clone();
        // monitor cache to handle evictions
        tokio::spawn(async move { clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });
        Ok(Self {
            iot_config_client,
            cache,
            refresh_interval: settings.region_params_refresh_interval(),
        })
    }

    pub async fn resolve_region_info(
        &self,
        region: ProtoRegion,
    ) -> Result<RegionParamsInfo, RegionCacheError> {
        match self.cache.get(&region).await {
            Some(hit) => {
                metrics::increment_counter!("oracles_iot_verifier_region_params_cache_hit");
                Ok(hit.value().clone())
            }
            _ => {
                match self
                    .iot_config_client
                    .clone()
                    .resolve_region_params(region)
                    .await
                {
                    Ok(res) => {
                        metrics::increment_counter!(
                            "oracles_iot_verifier_region_params_cache_miss"
                        );
                        self.cache
                            .insert(region, res.clone(), self.refresh_interval)
                            .await;
                        Ok(res)
                    }
                    Err(err) => Err(RegionCacheError::IotConfigClient(err)),
                }
            }
        }
    }
}
