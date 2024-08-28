use super::{call_with_retry, ClientError, Settings, CACHE_EVICTION_FREQUENCY};
use async_trait::async_trait;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::{mobile_config, Channel},
    Message, ServiceProvider,
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};

#[async_trait]
pub trait CarrierServiceVerifier {
    type Error;

    async fn payer_key_to_service_provider_name(&self, payer: &str) -> Result<String, Self::Error>;

    async fn payer_key_to_service_provider(
        &self,
        payer: &str,
    ) -> Result<ServiceProvider, Self::Error>;
}
#[derive(Clone)]
pub struct CarrierServiceClient {
    client: mobile_config::CarrierServiceClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    cache: Arc<Cache<String, String>>,
    cache_ttl: Duration,
}

#[async_trait]
impl CarrierServiceVerifier for CarrierServiceClient {
    type Error = ClientError;

    async fn payer_key_to_service_provider_name(&self, payer: &str) -> Result<String, ClientError> {
        if let Some(carrier_found) = self.cache.get(&payer.to_string()).await {
            return Ok(carrier_found.value().clone());
        }

        let mut request = mobile_config::CarrierKeyToEntityReqV1 {
            pubkey: payer.to_string(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(?payer, "getting service provider for payer key");
        let response = match call_with_retry!(self.client.clone().key_to_entity(request.clone())) {
            Ok(verify_res) => {
                let response = verify_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response.entity_key.clone()
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                Err(ClientError::UnknownServiceProvider(payer.to_string()))?
            }
            Err(status) => Err(status)?,
        };
        self.cache
            .insert(payer.to_string(), response.clone(), self.cache_ttl)
            .await;
        Ok(response)
    }

    async fn payer_key_to_service_provider(
        &self,
        payer: &str,
    ) -> Result<ServiceProvider, ClientError> {
        Ok(self
            .payer_key_to_service_provider_name(payer)
            .await?
            .parse()
            .map_err(|_| ClientError::UnknownServiceProvider(payer.to_string()))?)
    }
}

impl CarrierServiceClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let cache = Arc::new(Cache::new());
        let cloned_cache = cache.clone();
        tokio::spawn(async move {
            cloned_cache
                .monitor(4, 0.25, CACHE_EVICTION_FREQUENCY)
                .await
        });

        Ok(Self {
            client: settings.connect_carrier_service_client(),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
            cache_ttl: settings.cache_ttl,
            cache,
        })
    }
}
