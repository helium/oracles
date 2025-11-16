use crate::{
    admin::AuthCache, sub_dao_epoch_reward_info, telemetry, verify_public_key, GrpcResult, Settings,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::TimestampEncode;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::sub_dao::{
        self, SubDaoEpochRewardInfoReqV1, SubDaoEpochRewardInfoResV1,
    },
};
use helium_proto::Message;
use helium_proto_crypto::MsgVerify;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct SubDaoService {
    auth_cache: AuthCache,
    metadata_pool: Pool<Postgres>,
    signing_key: Arc<Keypair>,
}

impl SubDaoService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        metadata_pool: Pool<Postgres>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            metadata_pool,
            signing_key: Arc::new(settings.signing_keypair()?),
        })
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }

    fn verify_request_signature<R>(&self, signer: &PublicKey, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.auth_cache
            .verify_signature(signer, request)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(())
    }

    fn verify_request_signature_for_info(
        &self,
        request: &SubDaoEpochRewardInfoReqV1,
    ) -> std::result::Result<(), Status> {
        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, request)
    }
}

#[tonic::async_trait]
impl sub_dao::sub_dao_server::SubDao for SubDaoService {
    async fn info(
        &self,
        request: Request<SubDaoEpochRewardInfoReqV1>,
    ) -> GrpcResult<SubDaoEpochRewardInfoResV1> {
        let request = request.into_inner();
        telemetry::count_request("sub_dao_reward_info", "info");
        custom_tracing::record("sub_dao", &request.sub_dao_address);
        custom_tracing::record("epoch", request.epoch);
        custom_tracing::record_b58("signer", &request.signer);

        self.verify_request_signature_for_info(&request)?;

        let epoch = request.epoch;
        let sub_dao = request.sub_dao_address;
        tracing::info!(sub_dao = %sub_dao, epoch = epoch, "fetching sub_dao epoch reward info");

        sub_dao_epoch_reward_info::db::get_info(&self.metadata_pool, epoch, &sub_dao)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "error fetching sub_dao epoch reward info");
                Status::internal("error fetching sub_dao epoch reward info")
            })?
            .map_or_else(
                || {
                    telemetry::count_epoch_chain_lookup("not-found");
                    Err(Status::not_found(epoch.to_string()))
                },
                |info| {
                    let info = info.into();
                    let mut res = SubDaoEpochRewardInfoResV1 {
                        info: Some(info),
                        timestamp: Utc::now().encode_timestamp(),
                        signer: self.signing_key.public_key().into(),
                        signature: vec![],
                    };
                    res.signature = self.sign_response(&res.encode_to_vec())?;
                    Ok(Response::new(res))
                },
            )
    }
}
