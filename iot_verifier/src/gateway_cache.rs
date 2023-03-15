use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use iot_config_client::{
    gateway_resolver::{GatewayInfo, GatewayInfoResolver},
    iot_config_client::{IotConfigClient, IotConfigClientError},
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

/// how long each cached items takes to expire ( 12 hours in seconds)
const CACHE_TTL: Duration = Duration::from_secs(12 * (60 * 60));
/// how often to evict expired items from the cache ( every 1 hour)
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

pub struct GatewayCache {
    pub iot_config_client: IotConfigClient,
    pub cache: Arc<Cache<PublicKeyBinary, GatewayInfo>>,
    pub cache_monitor: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayCacheError {
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
    #[error("error querying iot config service")]
    IotConfigClient(#[from] IotConfigClientError),
}

impl GatewayCache {
    pub fn from_settings(iot_config_client: IotConfigClient) -> Result<Self, GatewayCacheError> {
        let cache = Arc::new(Cache::<PublicKeyBinary, GatewayInfo>::new());
        let clone = cache.clone();
        // monitor cache to handle evictions
        let cache_monitor =
            tokio::spawn(async move { clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });
        Ok(Self {
            iot_config_client,
            cache,
            cache_monitor,
        })
    }

    pub async fn prewarm(&self) -> anyhow::Result<()> {
        tracing::info!("starting prewarming gateway cache");
        let mut gw_stream = self.iot_config_client.clone().gateway_stream().await?;
        while let Some(gateway_info) = gw_stream.next().await {
            _ = self.insert(gateway_info).await;
        }
        tracing::info!("completed prewarming gateway cache");
        Ok(())
    }

    pub async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, GatewayCacheError> {
        match self.cache.get(address).await {
            Some(hit) => {
                metrics::increment_counter!("oracles_iot_verifier_gateway_cache_hit");
                Ok(hit.value().clone())
            }
            _ => {
                match self
                    .iot_config_client
                    .clone()
                    .resolve_gateway_info(address)
                    .await
                {
                    Ok(res) => {
                        tracing::debug!("cache miss: {:?}", address);
                        metrics::increment_counter!("oracles_iot_verifier_gateway_cache_miss");
                        _ = self.insert(res.clone()).await;
                        Ok(res)
                    }
                    _ => Err(GatewayCacheError::GatewayNotFound(address.clone())),
                }
            }
        }
    }

    pub async fn insert(&self, gateway: GatewayInfo) -> anyhow::Result<()> {
        self.cache
            .insert(gateway.address.clone(), gateway, CACHE_TTL)
            .await;
        Ok(())
    }
}
