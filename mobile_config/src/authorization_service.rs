use crate::{
    key_cache::{KeyCache, KeyRole},
    telemetry, verify_public_key, GrpcResult,
};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{self, AuthorizationListReqV1, AuthorizationListResV1, authorization_list_req_v1::NetworkKeyRole},
    Message,
};
use tonic::{Request, Response, Status};

pub struct AuthorizationService {
    key_cache: KeyCache,
    signing_key: Keypair,
}

impl AuthorizationService {
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
}

#[tonic::async_trait]
impl mobile_config::Authorization for AuthorizationService {
    async fn list(&self, request: Request<AuthorizationListReqV1>) -> GrpcResult<AuthorizationListResV1> {
        let request = request.into_inner();
        telemetry::count_request("authorization", "list");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("listing registered router keys");

        let requested_role: NetworkKeyRole = request.role().into();

        let registered_keys_by_role = self
            .key_cache
            .get_keys_by_role(requested_role.into())
            .into_iter()
            .map(|key| key.into())
            .collect();

        let mut response = AuthorizationListResV1 {
            pubkeys: registered_keys_by_role,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response.encode_to_vec())?;
        Ok(Response::new(response))
    }
}

impl From<NetworkKeyRole> for KeyRole {
    fn from(role: NetworkKeyRole) -> Self {
        match role {
            NetworkKeyRole::Router => KeyRole::Router,
            NetworkKeyRole::Carrier => KeyRole::Carrier,
        }
    }
}
