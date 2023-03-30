use crate::{region_map::RegionMap, GrpcResult, GrpcStreamResult, Settings};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{
        self, GatewayInfoReqV1, GatewayInfoResV1, GatewayInfoStreamReqV1, GatewayInfoStreamResV1,
        GatewayLocationReqV1, GatewayLocationResV1, GatewayRegionParamsReqV1,
        GatewayRegionParamsResV1,
    },
    Message,
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

        let gateway_address: &PublicKeyBinary = &request.gateway.into();

        let location = self
            .follower_service
            .clone()
            .resolve_gateway_info(gateway_address)
            .await
            .map_err(|_| Status::internal(format!("error retrieving gateway {gateway_address}")))
            .and_then(|info| {
                info.location
                    .ok_or_else(|| Status::not_found(format!("{gateway_address} not asserted")))
            })?;

        let location = Cell::from_raw(location)
            .map_err(|_| {
                Status::internal(format!(
                    "invalid h3 index location {location} for {gateway_address}"
                ))
            })?
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

        let pubkey: &PublicKeyBinary = &pubkey.into();
        tracing::debug!(pubkey = pubkey.to_string(), "fetching region params");

        let default_region = request.region();

        let (region, gain) = match self
            .follower_service
            .clone()
            .resolve_gateway_info(pubkey)
            .await
        {
            Err(_) => {
                tracing::debug!(
                    pubkey = pubkey.to_string(),
                    default_region = default_region.to_string(),
                    "error retrieving gateway from chain"
                );
                (default_region, 0)
            }
            Ok(GatewayInfo { location, gain, .. }) => {
                let region = match location {
                    None => {
                        tracing::debug!(
                            pubkey = pubkey.to_string(),
                            default_region = default_region.to_string(),
                            "no asserted location"
                        );
                        default_region
                    }
                    Some(location) => match Cell::from_raw(location) {
                        Ok(h3_location) => self
                            .region_map
                            .get_region(h3_location)
                            .await
                            .unwrap_or_else(|| {
                                tracing::debug!(
                                    pubkey = pubkey.to_string(),
                                    location = location,
                                    "gateway region lookup failed for assert location"
                                );
                                default_region
                            }),
                        Err(_) => {
                            tracing::debug!(
                                pubkey = pubkey.to_string(),
                                location = location,
                                "gateway asserted location is invalid h3 index"
                            );
                            default_region
                        }
                    },
                };
                (region, gain)
            }
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
        tracing::debug!(
            pubkey = pubkey.to_string(),
            region = region.to_string(),
            "returning region params"
        );
        Ok(Response::new(resp))
    }

    // placeholder implementation
    async fn info(&self, _request: Request<GatewayInfoReqV1>) -> GrpcResult<GatewayInfoResV1> {
        Ok(Response::new(GatewayInfoResV1 {
            timestamp: Utc::now().timestamp() as u64,
            info: None,
            signature: vec![],
        }))
    }

    // placeholder implementation
    type info_streamStream = GrpcStreamResult<GatewayInfoStreamResV1>;
    async fn info_stream(
        &self,
        _request: Request<GatewayInfoStreamReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let (_tx, rx) = tokio::sync::mpsc::channel(20);

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}
