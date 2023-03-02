use crate::Settings;
use helium_crypto::PublicKeyBinary;
use helium_proto::Region as ProtoRegion;
use iot_config_client::{
    iot_config_client::{IotConfigClient, IotConfigClientError},
    region_params_resolver::{RegionParamsInfo, RegionParamsResolver},
};
use retainer::Cache;
use std::time::Duration;

const CACHE_TTL: u64 = 86400;

pub struct RegionCache {
    pub iot_config_client: IotConfigClient,
    pub cache: Cache<ProtoRegion, RegionParamsInfo>,
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
    pub fn from_settings(settings: &Settings) -> Result<Self, RegionCacheError> {
        let iot_config_client = IotConfigClient::from_settings(&settings.iot_config_client)?;
        let cache = Cache::<ProtoRegion, RegionParamsInfo>::new();
        Ok(Self {
            iot_config_client,
            cache,
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
                            .insert(region, res.clone(), Duration::from_secs(CACHE_TTL))
                            .await;
                        Ok(res)
                    }
                    _ => Err(RegionCacheError::RegionNotFound(region)),
                }
            }
        }
    }
}
