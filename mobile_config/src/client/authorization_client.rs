use super::{call_with_retry, ClientError, Settings, CACHE_EVICTION_FREQUENCY};
use async_trait::async_trait;
use file_store_helium_proto::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{services::mobile_config, Message};
use retainer::Cache;
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

#[async_trait]
pub trait AuthorizationVerifier: Send + Sync + 'static {
    async fn verify_authorized_key(
        &self,
        pubkey: &PublicKeyBinary,
        role: mobile_config::NetworkKeyRole,
    ) -> Result<bool, ClientError>;
}

#[derive(Clone)]
pub struct AuthorizationClient {
    client: mobile_config::AuthorizationClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    cache: Arc<Cache<(PublicKeyBinary, mobile_config::NetworkKeyRole), bool>>,
    cache_ttl: Duration,
}

impl AuthorizationClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let cache = Arc::new(Cache::new());
        let cloned_cache = cache.clone();
        tokio::spawn(async move {
            cloned_cache
                .monitor(4, 0.25, CACHE_EVICTION_FREQUENCY)
                .await
        });

        Ok(Self {
            client: settings.connect_authorization_client(),
            signing_key: settings.signing_keypair.clone(),
            config_pubkey: settings.config_pubkey.clone(),
            cache_ttl: settings.cache_ttl,
            cache,
        })
    }
}

#[async_trait]
impl AuthorizationVerifier for AuthorizationClient {
    async fn verify_authorized_key(
        &self,
        pubkey: &PublicKeyBinary,
        role: mobile_config::NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        if let Some(registered) = self.cache.get(&(pubkey.clone(), role)).await {
            return Ok(*registered.value());
        }

        let mut request = mobile_config::AuthorizationVerifyReqV1 {
            pubkey: pubkey.clone().into(),
            role: role.into(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(pubkey = pubkey.to_string(), role = ?role, "verifying authorized key registered");
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
            .insert((pubkey.clone(), role), response, self.cache_ttl)
            .await;

        Ok(response)
    }
}
