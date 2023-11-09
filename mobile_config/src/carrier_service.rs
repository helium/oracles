use crate::{key_cache::KeyCache, telemetry, verify_public_key, GrpcResult};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{self, CarrierKeyToEntityReqV1, CarrierKeyToEntityResV1},
    Message,
};
use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct CarrierService {
    key_cache: KeyCache,
    pool: Pool<Postgres>,
    signing_key: Keypair,
}

impl CarrierService {
    pub fn new(key_cache: KeyCache, pool: Pool<Postgres>, signing_key: Keypair) -> Self {
        Self {
            key_cache,
            pool,
            signing_key,
        }
    }

    fn verify_request_signature<R>(&self, signer: &PublicKey, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self.key_cache.verify_signature(signer, request).is_ok() {
            tracing::info!(signer = signer.to_string(), "request authorized");
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }

    async fn key_to_entity(&self, pubkey: &String) -> Result<String, Status> {
        let entity_key = sqlx::query_scalar::<_, String>(
            " select entity_key from carrier_keys where pubkey = $1 ",
        )
        .bind(pubkey)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("carrier entity key not found"))?;
        Ok(entity_key)
    }
}

#[tonic::async_trait]
impl mobile_config::CarrierService for CarrierService {
    async fn key_to_entity(
        &self,
        request: Request<CarrierKeyToEntityReqV1>,
    ) -> GrpcResult<CarrierKeyToEntityResV1> {
        let request = request.into_inner();
        telemetry::count_request("carrier_service", "key_to_entity");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let entity_key = self.key_to_entity(&request.pubkey).await?;
        let mut response = CarrierKeyToEntityResV1 {
            entity_key,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response.encode_to_vec())?;
        Ok(Response::new(response))
    }
}
