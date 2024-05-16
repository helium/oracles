use crate::{key_cache::KeyCache, telemetry, verify_public_key, GrpcResult};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{self, EntityVerifyReqV1, EntityVerifyResV1},
    Message,
};
use sqlx::{Pool, Postgres, Row};
use tonic::{Request, Response, Status};

pub struct EntityService {
    key_cache: KeyCache,
    metadata_pool: Pool<Postgres>,
    signing_key: Keypair,
}

impl EntityService {
    pub fn new(key_cache: KeyCache, metadata_pool: Pool<Postgres>, signing_key: Keypair) -> Self {
        Self {
            key_cache,
            metadata_pool,
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

    async fn verify_entity(&self, entity_id: &[u8]) -> Result<bool, Status> {
        let row = sqlx::query(" select exists(select 1 from key_to_assets where entity_key = $1) ")
            .bind(entity_id)
            .fetch_one(&self.metadata_pool)
            .await
            .map_err(|_| Status::internal("error verifying entity on-chain"))?;
        Ok(row.get("exists"))
    }
}

#[tonic::async_trait]
impl mobile_config::Entity for EntityService {
    async fn verify(&self, request: Request<EntityVerifyReqV1>) -> GrpcResult<EntityVerifyResV1> {
        let request = request.into_inner();
        telemetry::count_request("entity", "verify");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("verifying rewardable entity on-chain");

        if self.verify_entity(&request.entity_id).await? {
            let mut response = EntityVerifyResV1 {
                timestamp: Utc::now().encode_timestamp(),
                signer: self.signing_key.public_key().into(),
                signature: vec![],
            };
            response.signature = self.sign_response(&response.encode_to_vec())?;
            Ok(Response::new(response))
        } else {
            Err(Status::not_found("Requested entity not on-chain"))
        }
    }
}
