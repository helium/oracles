use crate::{region_map::RegionMap, GrpcResult, Settings};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::future::TryFutureExt;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, GatewayLocationReqV1, GatewayLocationResV1, GatewayRegionParamsReqV1,
        GatewayRegionParamsResV1,
    },
    Message, Region,
};
use hextree::Cell;
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use tonic::{Request, Response, Status};

pub struct GatewayService {
    follower_service: FollowerService,
    region_map: RegionMap,
    signing_key: Keypair,
}

impl GatewayService {
    pub fn new(settings: &Settings, region_map: RegionMap) -> Result<Self> {
        Ok(Self {
            follower_service: FollowerService::from_settings(&settings.follower),
            region_map,
            signing_key: settings.signing_keypair()?,
        })
    }
}

#[tonic::async_trait]
impl iot_config::Gateway for GatewayService {
    async fn location(
        &self,
        request: Request<GatewayLocationReqV1>,
    ) -> GrpcResult<GatewayLocationResV1> {
        // Should this rpc be admin-authorized only or should a requesting pubkey
        // field be added to the request to do basic signature verification, allowing
        // open access but discourage endpoint abuse?
        let request = request.into_inner();

        let location = self
            .follower_service
            .clone()
            .resolve_gateway_info(&request.gateway.into())
            .and_then(|info| async move {
                info.location.ok_or(node_follower::Error::GatewayNotFound(
                    "unasserted".to_string(),
                ))
            })
            .await
            .map_err(|_| Status::internal("gateway lookup error"))?
            .to_string();

        Ok(Response::new(GatewayLocationResV1 { location }))
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
}
