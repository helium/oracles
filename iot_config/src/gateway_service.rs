use crate::{
    region_map::{self, RegionMap},
    GrpcResult, Settings,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, GatewayLoadRegionReqV1, GatewayLoadRegionResV1, GatewayLocationReqV1,
        GatewayLocationResV1, GatewayRegionParamsReqV1, GatewayRegionParamsResV1,
    },
    Message, Region,
};
use hextree::Cell;
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
        let pool = settings.database.connect(10).await?;
        let region_map = RegionMap::new(&pool).await?;
        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            follower_service: FollowerService::from_settings(&settings.follower),
            pool,
            region_map,
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
    async fn location(
        &self,
        _request: Request<GatewayLocationReqV1>,
    ) -> GrpcResult<GatewayLocationResV1> {
        unimplemented!()
    }

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

        let region = match location {
            Some(location) => {
                let h3_location = Cell::from_raw(location).map_err(|_| {
                    Status::internal("gateway asserted location is not a valid h3 index")
                })?;
                self.region_map
                    .get_region(h3_location)
                    .await
                    .ok_or_else(|| Status::internal("region lookup failed for assert location"))?
            }
            None => Region::from_i32(request.region)
                .ok_or_else(|| Status::invalid_argument("invalid default region"))?,
        };

        let params = self.region_map.get_params(&region).await;

        let mut resp = GatewayRegionParamsResV1 {
            region: region.into(),
            params,
            gain: gain as u64,
            signature: vec![],
        };
        resp.signature = self
            .signing_key
            .sign(&resp.encode_to_vec())
            .map_err(|_| Status::internal("resp signing error"))?;
        Ok(Response::new(resp))
    }

    async fn load_region(
        &self,
        request: Request<GatewayLoadRegionReqV1>,
    ) -> GrpcResult<GatewayLoadRegionResV1> {
        let request = request.into_inner();
        let req = self.verify_admin_signature(request)?;

        let region = Region::from_i32(req.region)
            .ok_or_else(|| Status::invalid_argument("invalid region"))?;

        let params = match req.params {
            Some(params) => params,
            None => return Err(Status::invalid_argument("missing region")),
        };

        let idz = if !req.hex_indexes.is_empty() {
            Some(req.hex_indexes.as_ref())
        } else {
            None
        };

        let updated_region = region_map::update_region(region, &params, idz, &self.pool)
            .await
            .map_err(|_| Status::internal("region update failed"))?;

        self.region_map.insert_params(region, params).await;
        if let Some(region_tree) = updated_region {
            tracing::debug!("New compacted region map with {} cells", region_tree.len());
            self.region_map.replace_tree(region_tree).await;
        }

        Ok(Response::new(GatewayLoadRegionResV1 {}))
    }
}
