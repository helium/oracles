use crate::Settings;
use helium_crypto::PublicKeyBinary;
use helium_proto::{BlockchainRegionParamV1, Region};
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfoResolver};
use retainer::Cache;
use std::time::Duration;

const CACHE_TTL: u64 = 86400;

#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub address: PublicKeyBinary,
    pub region: Region,
    pub region_params: Vec<BlockchainRegionParamV1>,
}

pub struct RegionCache {
    pub follower_service: FollowerService,
    pub cache: Cache<PublicKeyBinary, RegionInfo>,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating region cache: {0}")]
pub struct NewGatewayCacheError(#[from] db_store::Error);

#[derive(thiserror::Error, Debug)]
#[error("gateway not found: {0}")]
pub struct GatewayNotFound(PublicKeyBinary);

impl RegionCache {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewGatewayCacheError> {
        let follower_service = FollowerService::from_settings(&settings.follower);
        let cache = Cache::<PublicKeyBinary, RegionInfo>::new();
        Ok(Self {
            follower_service,
            cache,
        })
    }

    // temporarily pulls region from the node follower
    // TODO: to be replaced with RPC to config service or to solana. TBC
    pub async fn resolve_region_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<RegionInfo, GatewayNotFound> {
        match self.cache.get(address).await {
            Some(hit) => {
                metrics::increment_counter!("oracles_iot_verifier_region_cache_hit");
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
                        metrics::increment_counter!("oracles_iot_verifier_region_cache_miss");
                        let region_info = RegionInfo {
                            address: res.address.clone().into(),
                            region: res.region,
                            region_params: res.region_params,
                        };
                        self.cache
                            .insert(
                                address.clone(),
                                region_info.clone(),
                                Duration::from_secs(CACHE_TTL),
                            )
                            .await;
                        Ok(region_info)
                    }
                    _ => Err(GatewayNotFound(address.clone())),
                }
            }
        }
    }
}
