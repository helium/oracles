use crate::{
    key_cache::{KeyCache, KeyType},
    GrpcResult,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Network, PublicKey, PublicKeyBinary};
use helium_proto::services::mobile_config::{
    self, AdminAddKeyReqV1, AdminKeyResV1, AdminRemoveKeyReqV1,
};
use tonic::{Request, Response, Status};

pub struct AdminService {
    key_cache: KeyCache,
    required_network: Network,
}

impl AdminService {
    pub fn new(key_cache: KeyCache, required_network: Network) -> Self {
        Self {
            key_cache,
            required_network,
        }
    }

    async fn verify_request_signature<R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.key_cache
            .verify_signature(KeyType::Administrator, request)
            .await
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
}

#[tonic::async_trait]
impl mobile_config::Admin for AdminService {
    async fn add_key(&self, request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let key_type = request.key_type().into();
        let pubkey = self
            .verify_public_key(request.pubkey.as_ref())
            .and_then(|pubkey| self.verify_network(pubkey))?;

        self.key_cache
            .insert_key(key_type, pubkey)
            .await
            .map_err(|_| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey add failed");
                Status::internal(format!(
                    "error saving requested key: {pubkey}",
                    pubkey = pubkey
                ))
            })?;

        Ok(Response::new(AdminKeyResV1 {}))
    }

    async fn remove_key(&self, request: Request<AdminRemoveKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;
        let pubkey = self.verify_public_key(request.pubkey.as_ref())?;

        self.key_cache.remove_key(&pubkey).await.map_err(|_| {
            let pubkey: PublicKeyBinary = request.pubkey.into();
            tracing::error!(pubkey = pubkey.to_string(), "pubkey remove failed");
            Status::internal(format!("error removing request key: {pubkey}"))
        })?;

        Ok(Response::new(AdminKeyResV1 {}))
    }
}
