use std::{collections::HashMap, str::FromStr};

use crate::{key_cache::KeyCache, telemetry, verify_public_key, GrpcResult};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    service_provider_promotions::Promotion,
    services::mobile_config::{
        self, CarrierIncentivePromotionListReqV1, CarrierIncentivePromotionListResV1,
        CarrierKeyToEntityReqV1, CarrierKeyToEntityResV1,
    },
    Message, ServiceProvider, ServiceProviderPromotions,
};
use sqlx::{prelude::FromRow, Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct CarrierService {
    key_cache: KeyCache,
    mobile_config_db: Pool<Postgres>,
    metadata_db: Pool<Postgres>,
    signing_key: Keypair,
}

impl CarrierService {
    pub fn new(
        key_cache: KeyCache,
        mobile_config_db: Pool<Postgres>,
        metadata_db: Pool<Postgres>,
        signing_key: Keypair,
    ) -> Self {
        Self {
            key_cache,
            mobile_config_db,
            metadata_db,
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
        .fetch_one(&self.mobile_config_db)
        .await
        .map_err(|_| Status::internal("carrier entity key not found"))?;
        Ok(entity_key)
    }

    async fn fetch_incentive_promotions(
        &self,
        timestamp: i64,
    ) -> Result<Vec<ServiceProviderPromotions>, Status> {
        #[derive(Debug, FromRow)]
        struct Local {
            carrier_name: String,
            incentive_escrow_fund_bps: i32,
            start_ts: i64,
            stop_ts: i64,
            shares: i32,
            promo_name: String,
        }

        let rows = sqlx::query_as::<_, Local>(
            r#"
                SELECT 
                    c.name as carrier_name, c.incentive_escrow_fund_bps, 
                    iep.carrier, iep.start_ts::bigint, iep.stop_ts::bigint, iep.shares, iep.name as promo_name
                FROM carriers c
                JOIN incentive_escrow_programs iep 
                    on c.address = iep.carrier
                WHERE 
                    iep.start_ts < $1
                    AND iep.stop_ts > $1
            "#,
        )
        .bind(timestamp)
        .fetch_all(&self.metadata_db)
        .await
        .map_err(|err| {
            tracing::error!(?err, "fetching incentive programs");
            Status::internal("could not fetch incentive programs")
        })?;

        let mut sp_promotions: HashMap<String, ServiceProviderPromotions> = HashMap::new();
        for row in rows {
            let m = sp_promotions.entry(row.carrier_name.clone()).or_default();
            m.service_provider = ServiceProvider::from_str(&row.carrier_name)
                .map_err(|err| Status::internal(format!("unknown carrier: {err:?}")))?
                .into();
            m.incentive_escrow_fund_bps = row.incentive_escrow_fund_bps as u32;
            m.promotions.push(Promotion {
                entity: row.promo_name,
                start_ts: row.start_ts as u64,
                end_ts: row.stop_ts as u64,
                shares: row.shares as u32,
            });
        }

        Ok(sp_promotions.into_values().collect())
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
        custom_tracing::record("pub_key", &request.pubkey);
        custom_tracing::record_b58("signer", &request.signer);

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

    async fn list_incentive_promotions(
        &self,
        request: Request<CarrierIncentivePromotionListReqV1>,
    ) -> GrpcResult<CarrierIncentivePromotionListResV1> {
        let request = request.into_inner();
        telemetry::count_request("carrier_service", "list_incentive_promotions");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let timestamp = request.timestamp;
        let promotions = self.fetch_incentive_promotions(timestamp as i64).await?;

        let mut response = CarrierIncentivePromotionListResV1 {
            service_provider_promotions: promotions,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        response.signature = self.sign_response(&response.encode_to_vec())?;

        Ok(Response::new(response))
    }
}
