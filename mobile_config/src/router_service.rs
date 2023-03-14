use crate::{
    key_cache::{KeyCache, KeyType},
    GrpcResult,
};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{
        self, RouterGetReqV1, RouterGetResV1, RouterListReqV1, RouterListResV1,
    },
    Message,
};
use tonic::{Request, Response, Status};

pub struct RouterService {
    key_cache: KeyCache,
    signing_key: Keypair,
}

impl RouterService {
    pub fn new(key_cache: KeyCache, signing_key: Keypair) -> Self {
        Self {
            key_cache,
            signing_key,
        }
    }

    async fn verify_request_signature<'a, R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .key_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
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
impl mobile_config::Router for RouterService {
    async fn get(&self, request: Request<RouterGetReqV1>) -> GrpcResult<RouterGetResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let pubkey = self.verify_public_key(&request.pubkey)?;

        tracing::debug!(
            pubkey = pubkey.to_string(),
            "checking for registered router key"
        );

        if self.key_cache.has_key(&pubkey).await {
            let mut response = RouterGetResV1 {
                pubkey: request.pubkey,
                timestamp: Utc::now().encode_timestamp(),
                signature: vec![],
            };
            response.signature = self.sign_response(&response)?;
            Ok(Response::new(response))
        } else {
            Err(Status::not_found(pubkey.to_string()))
        }
    }

    async fn list(&self, request: Request<RouterListReqV1>) -> GrpcResult<RouterListResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        tracing::debug!("listing registered router keys");

        let registered_routers = self
            .key_cache
            .list_keys(KeyType::PacketRouter)
            .await
            .into_iter()
            .map(|key| key.into())
            .collect();

        let mut response = RouterListResV1 {
            routers: registered_routers,
            timestamp: Utc::now().encode_timestamp(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response)?;
        Ok(Response::new(response))
    }
}
