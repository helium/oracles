use crate::{GrpcResult, Settings};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{self, GatewayRegionParamsReqV1, GatewayRegionParamsResV1},
    Message,
};
use node_follower::{
    follower_service::FollowerService,
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
};
use tonic::{Request, Response, Status};

pub struct GatewayService {
    follower_service: FollowerService,
    signing_key: Keypair,
}

impl GatewayService {
    pub fn new(settings: &Settings) -> Result<Self> {
        Ok(Self {
            follower_service: FollowerService::from_settings(&settings.follower),
            signing_key: settings.signing_keypair()?,
        })
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
}
