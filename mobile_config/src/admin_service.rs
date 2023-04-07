use crate::{
    key_cache::{self, CacheKeys, KeyCache, KeyType},
    settings::Settings,
    GrpcResult,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::future::TryFutureExt;
use helium_crypto::{Keypair, Network, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::mobile_config::{self, AdminAddKeyReqV1, AdminKeyResV1, AdminRemoveKeyReqV1},
    Message,
};
use sqlx::{Pool, Postgres};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

pub struct AdminService {
    key_cache: KeyCache,
    key_cache_updater: watch::Sender<CacheKeys>,
    pool: Pool<Postgres>,
    required_network: Network,
    signing_key: Keypair,
}

impl AdminService {
    pub fn new(
        settings: &Settings,
        key_cache: KeyCache,
        key_cache_updater: watch::Sender<CacheKeys>,
        pool: Pool<Postgres>,
    ) -> Result<Self> {
        Ok(Self {
            key_cache,
            key_cache_updater,
            pool,
            required_network: settings.network,
            signing_key: settings.signing_keypair()?,
        })
    }

    fn verify_admin_request_signature<R>(
        &self,
        signer: &PublicKey,
        request: &R,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.key_cache
            .verify_signature_with_type(KeyType::Administrator, signer, request)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(())
    }

    fn verify_network(&self, public_key: PublicKey) -> Result<PublicKey, Status> {
        if self.required_network == public_key.network {
            Ok(public_key)
        } else {
            Err(Status::invalid_argument(format!(
                "invalid network: {}",
                public_key.network
            )))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> Result<PublicKey, Status> {
        PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
    }

    fn sign_response<R>(&self, response: &R) -> Result<Vec<u8>, Status>
    where
        R: Message,
    {
        self.signing_key
            .sign(&response.encode_to_vec())
            .map_err(|_| Status::internal("response signing error"))
    }
}

#[tonic::async_trait]
impl mobile_config::Admin for AdminService {
    async fn add_key(&self, request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        let key_type = request.key_type().into();
        let pubkey = self
            .verify_public_key(request.pubkey.as_ref())
            .and_then(|pubkey| self.verify_network(pubkey))?;

        key_cache::db::insert_key(request.pubkey.clone().into(), key_type, &self.pool)
            .and_then(|_| async move {
                if self.key_cache_updater.send_if_modified(|cache| {
                    if let std::collections::hash_map::Entry::Vacant(key) = cache.entry(pubkey) {
                        key.insert(key_type);
                        true
                    } else {
                        false
                    }
                }) {
                    Ok(())
                } else {
                    Err(anyhow!("key already registered"))
                }
            })
            .map_err(|err| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey add failed");
                Status::internal(format!("error saving request key: {pubkey}, {err:?}"))
            })
            .await?;

        let mut resp = AdminKeyResV1 {
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;
        Ok(Response::new(resp))
    }

    async fn remove_key(&self, request: Request<AdminRemoveKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        key_cache::db::remove_key(request.pubkey.clone().into(), &self.pool)
            .and_then(|deleted| async move {
                match deleted {
                    Some((pubkey, _key_type)) => {
                        self.key_cache_updater.send_modify(|cache| {
                            cache.remove(&pubkey);
                        });
                        Ok(())
                    }
                    None => Ok(()),
                }
            })
            .map_err(|_| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey remove failed");
                Status::internal(format!("error removing request key: {pubkey}"))
            })
            .await?;

        let mut resp = AdminKeyResV1 {
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;
        Ok(Response::new(resp))
    }
}
