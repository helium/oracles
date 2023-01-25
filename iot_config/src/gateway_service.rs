use crate::{
    region_map::{self, RegionMap},
    GrpcResult, Settings,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, GatewayRegionParamsReqV1, GatewayRegionParamsResV1, LoadRegionReqV1, LoadRegionResV1,
    },
    Message,
};
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct GatewayService {
    admin_pubkey: PublicKey,
    follower_service: FollowerService,
    pool: Pool<Postgres>,
    region_map: RegionMap,
    signing_key: Keypair,
}

impl GatewayService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            follower_service: FollowerService::from_settings(&settings.follower),
            pool: settings.database.connect(10).await?,
            region_map: RegionMap::new(),
            signing_key: settings.signing_keypair()?,
        })
    }

    fn verify_admin_signature<R>(&self, request: R) -> Result<R, Status>
    where
        R: MsgVerify,
    {
        request
            .verify(&self.admin_pubkey)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(request)
    }
}

#[tonic::async_trait]
impl iot_config::Gateway for GatewayService {
    async fn region_params(
        &self,
        request: Request<GatewayRegionParamsReqV1>,
    ) -> GrpcResult<GatewayRegionParamsResV1> {
        let request = request.into_inner();

        let pubkey = PublicKey::try_from(request.address.clone())
            .map_err(|_| Status::invalid_argument("invalid gateway address"))?;
        request
            .verify(&pubkey)
            .map_err(|_| Status::permission_denied("invalid request signature"))?;

        let GatewayInfo { location, gain, .. } = self
            .follower_service
            .clone()
            .resolve_gateway_info(&pubkey.into())
            .await
            .map_err(|_| Status::internal("gateway lookup error"))?;

        match location {
            Some(hex) => tracing::info!("Gateway located at hex index {hex}"),
            None => tracing::info!("Gateway location undefined"),
        }

        let mut resp = GatewayRegionParamsResV1 {
            region: helium_proto::Region::Us915.into(),
            params: None,
            gain: gain as u64,
            signature: vec![],
        };
        resp.signature = self
            .signing_key
            .sign(&resp.encode_to_vec())
            .map_err(|_| Status::internal("resp signing error"))?;
        Ok(Response::new(resp))
    }

    async fn load_region(&self, request: Request<LoadRegionReqV1>) -> GrpcResult<LoadRegionResV1> {
        let request = request.into_inner();
        let req = self.verify_admin_signature(request)?;

        let params = match req.params {
            Some(params) => params,
            None => return Err(Status::invalid_argument("missing region")),
        };

        let indexes = match req.hex_indexes {
            Some(indexes) => indexes,
            None => {
                tracing::debug!("h3 region index update skipped");
                vec![]
            }
        };

        let _updated_region = region_map::update_region(req.region, params, &indexes, &self.pool)
            .await
            .map_err(|_| Status::internal("region update failed"))?;

        let new_region_map = region_map::build_region_map(&self.pool)
            .await
            .map_err(|_| Status::internal("region hextree failed to build"))?;

        self.region_map.swap(new_region_map).await;

        Ok(Response::new(LoadRegionResV1 {}))
    }
}
