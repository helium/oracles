use crate::{key_cache::KeyCache, telemetry, verify_public_key, GrpcResult, KeyRole};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{
        self, AuthorizationListReqV1, AuthorizationListResV1, AuthorizationVerifyReqV1,
        AuthorizationVerifyResV1, NetworkKeyRole,
    },
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
    async fn verify(
        &self,
        request: Request<AuthorizationVerifyReqV1>,
    ) -> GrpcResult<AuthorizationVerifyResV1> {
        let request = request.into_inner();
        telemetry::count_request("authorization", "verify");
        custom_tracing::record_b58("pub_key", &request.pubkey);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let requested_role: KeyRole = request.role().into();
        let requested_key = verify_public_key(&request.pubkey)?;
        tracing::debug!(key = %requested_key, role = %requested_role, "verifying authorized key with role");

        if self
            .key_cache
            .verify_key_by_role(&requested_key, requested_role)
        {
            let mut response = AuthorizationVerifyResV1 {
                timestamp: Utc::now().encode_timestamp(),
                signer: self.signing_key.public_key().into(),
                signature: vec![],
            };
            response.signature = self.sign_response(&response.encode_to_vec())?;
            Ok(Response::new(response))
        } else {
            Err(Status::not_found(format!(
                "Requested key/role not registered {requested_key}: {requested_role}"
            )))
        }
    }

    async fn list(
        &self,
        request: Request<AuthorizationListReqV1>,
    ) -> GrpcResult<AuthorizationListResV1> {
        let request = request.into_inner();
        telemetry::count_request("authorization", "list");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("listing registered authorized network keys");

        let requested_role: KeyRole = request.role().into();

        let registered_keys_by_role = self
            .key_cache
            .get_keys_by_role(requested_role)
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
            NetworkKeyRole::MobileRouter => KeyRole::Router,
            NetworkKeyRole::MobileCarrier => KeyRole::Carrier,
            NetworkKeyRole::MobilePcs => KeyRole::Pcs,
        }
    }
}
