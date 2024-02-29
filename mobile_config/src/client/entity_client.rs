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
pub trait EntityVerifier {
    type Error;

    async fn verify_rewardable_entity(&self, entity_id: &[u8]) -> Result<bool, Self::Error>;
}

#[derive(Clone)]
pub struct EntityClient {
    client: mobile_config::EntityClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    cache: Arc<Cache<Vec<u8>, bool>>,
    cache_ttl: Duration,
}

#[async_trait]
impl EntityVerifier for EntityClient {
    type Error = ClientError;

    async fn verify_rewardable_entity(&self, entity_id: &[u8]) -> Result<bool, ClientError> {
        let entity_id = entity_id.to_vec();
        if let Some(entity_found) = self.cache.get(&entity_id).await {
            return Ok(*entity_found.value());
        }

        let mut request = mobile_config::EntityVerifyReqV1 {
            entity_id: entity_id.clone(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(?entity_id, "verifying entity on-chain");
        let response = match call_with_retry!(self.client.clone().verify(request.clone())) {
            Ok(verify_res) => {
                let response = verify_res.into_inner();
                response.verify(&self.config_pubkey)?;
                true
            }
            Err(status) if status.code() == tonic::Code::NotFound => false,
            Err(status) => Err(status)?,
        };

        self.cache
            .insert(entity_id.clone(), response, self.cache_ttl)
            .await;

        Ok(response)
    }
}

impl EntityClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let cache = Arc::new(Cache::new());
        let cloned_cache = cache.clone();
        tokio::spawn(async move {
            cloned_cache
                .monitor(4, 0.25, CACHE_EVICTION_FREQUENCY)
                .await
        });

        Ok(Self {
            client: settings.connect_entity_client(),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
            cache_ttl: settings.cache_ttl(),
            cache,
        })
    }
}
