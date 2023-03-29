use crate::iot_config_client::IotConfigClientError;
use async_trait::async_trait;
use helium_proto::{
    services::iot_config::RegionParamsResV1 as RegionParamsProto, BlockchainRegionParamV1, Region,
};

#[async_trait]
pub trait RegionParamsResolver {
    async fn resolve_region_params(
        &mut self,
        region: Region,
    ) -> Result<RegionParamsInfo, IotConfigClientError>;
}

#[derive(Debug, Clone)]
pub struct RegionParamsInfo {
    pub region: Region,
    pub region_params: Vec<BlockchainRegionParamV1>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegionParamsResolverError {
    #[error("unsupported region {0}")]
    Region(String),
}

impl TryFrom<RegionParamsProto> for RegionParamsInfo {
    type Error = RegionParamsResolverError;
    fn try_from(v: RegionParamsProto) -> Result<Self, RegionParamsResolverError> {
        let region: Region = Region::from_i32(v.region)
            .ok_or_else(|| RegionParamsResolverError::Region(format!("{:?}", v.region)))?;
        let region_params = v
            .params
            .ok_or_else(|| {
                RegionParamsResolverError::Region("failed to get region params".to_string())
            })?
            .region_params;
        Ok(Self {
            region,
            region_params,
        })
    }
}
