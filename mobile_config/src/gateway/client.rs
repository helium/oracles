use crate::client::{call_with_retry, ClientError, Settings};
use crate::gateway::service::info::{GatewayInfo, GatewayInfoStream};
use file_store_helium_proto::traits::MsgVerify;
use futures::stream::{self, StreamExt};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::mobile_config::{self, DeviceType},
    Message,
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub struct GatewayClient {
    pub client: mobile_config::GatewayClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    batch_size: u32,
    cache: Arc<Cache<PublicKeyBinary, Option<GatewayInfo>>>,
    cache_ttl: Duration,
}

impl GatewayClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let cache = Arc::new(Cache::new());
        let cloned_cache = cache.clone();
        tokio::spawn(async move {
            cloned_cache
                .monitor(4, 0.25, CACHE_EVICTION_FREQUENCY)
                .await
        });

        Ok(Self {
            client: settings.connect_gateway_client(),
            signing_key: settings.signing_keypair.clone(),
            config_pubkey: settings.config_pubkey.clone(),
            batch_size: settings.batch_size,
            cache_ttl: settings.cache_ttl,
            cache,
        })
    }
}

#[async_trait::async_trait]
pub trait GatewayInfoResolver: Clone + Send + Sync + 'static {
    async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, ClientError>;

    async fn stream_gateways_info(
        &mut self,
        device_types: &[DeviceType],
    ) -> Result<GatewayInfoStream, ClientError>;
}

#[async_trait::async_trait]
impl GatewayInfoResolver for GatewayClient {
    async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, ClientError> {
        if let Some(cached_response) = self.cache.get(address).await {
            return Ok(cached_response.value().clone());
        }

        let mut request = mobile_config::GatewayInfoReqV1 {
            address: address.clone().into(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(pubkey = address.to_string(), "fetching gateway info");
        let response = match call_with_retry!(self.client.clone().info_v2(request.clone())) {
            Ok(info_res) => {
                let response = info_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response.info.map(GatewayInfo::try_from).transpose()?
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => Err(status)?,
        };

        self.cache
            .insert(address.clone(), response.clone(), self.cache_ttl)
            .await;

        Ok(response)
    }

    /// Returns all gateways if device_types is empty
    /// Otherwise, only selected device_types
    async fn stream_gateways_info(
        &mut self,
        device_types: &[DeviceType],
    ) -> Result<GatewayInfoStream, ClientError> {
        let mut req = mobile_config::GatewayInfoStreamReqV2 {
            batch_size: self.batch_size,
            device_types: device_types.iter().map(|v| DeviceType::into(*v)).collect(),
            min_updated_at: 0,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        tracing::debug!("fetching gateway info stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let res_stream = call_with_retry!(self.client.info_stream_v2(req.clone()))?
            .into_inner()
            .filter_map(|res| async move { res.ok() })
            .map(move |res| (res, pubkey.clone()))
            .filter_map(|(res, pubkey)| async move {
                match res.verify(&pubkey) {
                    Ok(()) => Some(res),
                    Err(_) => None,
                }
            })
            .flat_map(|res| stream::iter(res.gateways))
            .map(GatewayInfo::try_from)
            .filter_map(|gateway| async move { gateway.ok() })
            .boxed();

        Ok(res_stream)
    }
}
