use crate::{
    key_cache::{KeyCache, KeyType},
    verify_public_key, GrpcResult,
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

    fn verify_request_signature<R>(&self, signer: &PublicKey, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self.key_cache.verify_signature(signer, request).is_ok() {
            tracing::debug!(signer = signer.to_string(), "request authorized");
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }

    fn verify_router_registered(&self, router: &[u8]) -> Result<(), Status> {
        let pubkey = verify_public_key(router)
            .map_err(|_| Status::invalid_argument("invalid router key"))?;
        tracing::debug!(
            pubkey = pubkey.to_string(),
            "checking for registered router key"
        );

        if self
            .key_cache
            .get_keys_by_type(KeyType::PacketRouter)
            .contains(&pubkey)
        {
            Ok(())
        } else {
            Err(Status::not_found(pubkey.to_string()))
        }
    }
}

#[tonic::async_trait]
impl mobile_config::Router for RouterService {
    async fn get(&self, request: Request<RouterGetReqV1>) -> GrpcResult<RouterGetResV1> {
        let request = request.into_inner();

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        self.verify_router_registered(&request.pubkey)?;
        let mut response = RouterGetResV1 {
            pubkey: request.pubkey,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response.encode_to_vec())?;
        Ok(Response::new(response))
    }

    async fn list(&self, request: Request<RouterListReqV1>) -> GrpcResult<RouterListResV1> {
        let request = request.into_inner();

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("listing registered router keys");

        let registered_routers = self
            .key_cache
            .get_keys_by_type(KeyType::PacketRouter)
            .into_iter()
            .map(|key| key.into())
            .collect();

        let mut response = RouterListResV1 {
            routers: registered_routers,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response.encode_to_vec())?;
        Ok(Response::new(response))
    }
}
