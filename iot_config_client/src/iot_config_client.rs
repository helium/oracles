use crate::{
    gateway_resolver::{self, GatewayInfo, GatewayInfoResolver},
    region_params_resolver::{self, RegionParamsInfo, RegionParamsResolver},
    GatewayInfoStream, Settings,
};
use anyhow::Result;
use futures::stream::{self, StreamExt};
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::services::{
    iot_config::{self, GatewayInfoReqV1, GatewayInfoStreamReqV1, RegionParamsReqV1},
    Channel,
};
use helium_proto::{Message, Region};
use std::sync::Arc;

type IotGatewayConfigClient = iot_config::GatewayClient<Channel>;
type IotAdminConfigClient = iot_config::admin_client::AdminClient<Channel>;

#[derive(Debug, Clone)]
pub struct IotConfigClient {
    keypair: Arc<Keypair>,
    pub gateway_client: IotGatewayConfigClient,
    pub region_client: IotAdminConfigClient,
    batch_size: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum IotConfigClientError {
    #[error("error resolving gateway info")]
    GatewayResolverError(#[from] gateway_resolver::GatewayResolverError),
    #[error("error resolving region params")]
    RegionParamsResolverError(#[from] region_params_resolver::RegionParamsResolverError),
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
    #[error("region params not found {0}")]
    RegionParamsNotFound(String),
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("keypair error: {0}")]
    KeypairError(#[from] Box<helium_crypto::Error>),
    #[error("signing error: {0}")]
    SignError(#[from] helium_crypto::Error),
}

#[async_trait::async_trait]
impl GatewayInfoResolver for IotConfigClient {
    async fn resolve_gateway_info(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, IotConfigClientError> {
        let mut req = GatewayInfoReqV1 {
            address: address.clone().into(),
            signature: vec![],
        };
        req.signature = self.keypair.sign(&req.encode_to_vec())?;
        let res = self.gateway_client.info(req).await?.into_inner();
        match res.info {
            Some(gateway_info) => Ok(gateway_info.try_into()?),
            _ => Err(IotConfigClientError::GatewayNotFound(address.clone())),
        }
    }
}

#[async_trait::async_trait]
impl RegionParamsResolver for IotConfigClient {
    async fn resolve_region_params(
        &mut self,
        region: Region,
    ) -> Result<RegionParamsInfo, IotConfigClientError> {
        let mut req = RegionParamsReqV1 {
            region: region.into(),
            signature: vec![],
        };
        req.signature = self.keypair.sign(&req.encode_to_vec())?;
        match self.region_client.region_params(req).await {
            Ok(region_params_info) => Ok(region_params_info.into_inner().try_into()?),
            _ => Err(IotConfigClientError::RegionParamsNotFound(format!(
                "{region:?}"
            ))),
        }
    }
}

impl IotConfigClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, IotConfigClientError> {
        let channel = settings.connect_endpoint();
        let keypair = settings.keypair()?;
        Ok(Self {
            keypair: Arc::new(keypair),
            gateway_client: settings.connect_iot_gateway_config(channel.clone()),
            region_client: settings.connect_iot_region_config(channel),
            batch_size: settings.batch,
        })
    }

    pub async fn gateway_stream(&mut self) -> Result<GatewayInfoStream, IotConfigClientError> {
        let mut req = GatewayInfoStreamReqV1 {
            batch_size: self.batch_size,
            signature: vec![],
        };
        req.signature = self.keypair.sign(&req.encode_to_vec())?;
        let gw_stream = self
            .gateway_client
            .info_stream(req)
            .await?
            .into_inner()
            .filter_map(|resp| async move { resp.ok() })
            .flat_map(|resp| stream::iter(resp.gateways.into_iter()))
            .filter_map(|resp| async move { GatewayInfo::try_from(resp).ok() })
            .boxed();

        Ok(gw_stream)
    }
}
