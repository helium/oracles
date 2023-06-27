use super::{ClientError, Settings, CACHE_EVICTION_FREQUENCY};
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::{mobile_config, Channel},
    Message,
};
use retainer::Cache;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct EntityClient {
    client: mobile_config::EntityClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    cache: Arc<Cache<Vec<u8>, bool>>,
    cache_ttl: Duration,
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

    pub async fn verify_rewardable_entity(&self, entity_id: &Vec<u8>) -> Result<bool, ClientError> {
        if let Some(entity_found) = self.cache.get(entity_id).await {
            return Ok(*entity_found.value());
        }

        let mut request = mobile_config::EntityVerifyReqV1 {
            entity_id: entity_id.clone(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(?entity_id, "verifying entity on-chain");
        let response = match self.client.clone().verify(request).await {
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
