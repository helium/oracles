use crate::{helius, Settings};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helius::GatewayInfo;
use retainer::Cache;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

// how long each cached items takes to expire ( 12 hours in seconds)
const CACHE_TTL: Duration = Duration::from_secs(12 * (60 * 60));
/// how often to evict expired items from the cache ( every 1 hour)
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

const HELIUS_DB_POOL_SIZE: usize = 100;

pub struct GatewayCache {
    pool: PgPool,
    pub cache: Arc<Cache<PublicKeyBinary, GatewayInfo>>,
    pub cache_monitor: JoinHandle<()>,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating gateway cache: {0}")]
pub struct NewGatewayCacheError(#[from] db_store::Error);

#[derive(thiserror::Error, Debug)]
#[error("gateway not found: {0}")]
pub struct GatewayNotFound(PublicKeyBinary);

impl GatewayCache {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewGatewayCacheError> {
        let pool = settings.database.connect(HELIUS_DB_POOL_SIZE).await?;
        let cache = Arc::new(Cache::<PublicKeyBinary, GatewayInfo>::new());
        let clone = cache.clone();
        // monitor cache to handle evictions
        let cache_monitor =
            tokio::spawn(async move { clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });

        Ok(Self {
            pool,
            cache,
            cache_monitor,
        })
    }

    pub async fn prewarm(&self) -> anyhow::Result<()> {
        let sql = r#"SELECT address, location, elevation, gain, is_full_hotspot FROM gateways"#;
        let mut rows = sqlx::query_as::<_, GatewayInfo>(sql).fetch(&self.pool);
        while let Some(gateway) = rows.try_next().await? {
            self.cache
                .insert(gateway.address.clone(), gateway.clone(), CACHE_TTL)
                .await;
        }
        Ok(())
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
            _ => match GatewayInfo::resolve_gateway(&self.pool, address.as_ref()).await {
                Ok(Some(res)) => {
                    metrics::increment_counter!("oracles_iot_verifier_gateway_cache_miss");
                    self.cache
                        .insert(address.clone(), res.clone(), CACHE_TTL)
                        .await;
                    Ok(res)
                }
                _ => Err(GatewayNotFound(address.clone())),
            },
        }
    }
}
