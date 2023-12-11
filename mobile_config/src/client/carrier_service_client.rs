use super::{call_with_retry, ClientError, Settings, CACHE_EVICTION_FREQUENCY};
use async_trait::async_trait;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::{mobile_config, Channel},
    Message,
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};

#[async_trait]
pub trait CarrierServiceVerifier {
    type Error;
    async fn key_to_rewardable_entity<'a>(&self, entity_id: &'a str)
        -> Result<String, Self::Error>;
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

    async fn key_to_rewardable_entity<'a>(&self, pubkey: &'a str) -> Result<String, ClientError> {
        if let Some(carrier_found) = self.cache.get(&pubkey.to_string()).await {
            return Ok(carrier_found.value().clone());
        }

        let mut request = mobile_config::CarrierKeyToEntityReqV1 {
            pubkey: pubkey.to_string(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(?pubkey, "getting entity key for carrier on-chain");
        let response = match call_with_retry!(self.client.clone().key_to_entity(request.clone())) {
            Ok(verify_res) => {
                let response = verify_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response.entity_key
            }
            Err(status) => Err(status)?,
        };
        self.cache
            .insert(pubkey.to_string(), response.clone(), self.cache_ttl)
            .await;
        Ok(response)
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
            cache_ttl: settings.cache_ttl(),
            cache,
        })
    }
}
