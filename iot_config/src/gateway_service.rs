use crate::{
    admin::AuthCache,
    gateway_info::{self, GatewayInfo},
    region_map::RegionMapReader,
    GrpcResult, GrpcStreamResult, Settings,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::stream::StreamExt;
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{
        self, GatewayInfoReqV1, GatewayInfoResV1, GatewayInfoStreamReqV1, GatewayInfoStreamResV1,
        GatewayLocationReqV1, GatewayLocationResV1, GatewayRegionParamsReqV1,
        GatewayRegionParamsResV1,
    },
    Message, Region,
};
use hextree::Cell;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct GatewayService {
    auth_cache: AuthCache,
    metadata_pool: Pool<Postgres>,
    region_map: RegionMapReader,
    signing_key: Arc<Keypair>,
}

impl GatewayService {
    pub fn new(
        settings: &Settings,
        metadata_pool: Pool<Postgres>,
        region_map: RegionMapReader,
        auth_cache: AuthCache,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            metadata_pool,
            region_map,
            signing_key: Arc::new(settings.signing_keypair()?),
        })
    }

    fn verify_public_key(&self, bytes: &[u8]) -> Result<PublicKey, Status> {
        PublicKey::try_from(bytes)
            .map_err(|_| Status::invalid_argument(format!("invalid public key: {bytes:?}")))
    }

    fn sign_response<R>(&self, response: &R) -> Result<Vec<u8>, Status>
    where
        R: Message,
    {
        self.signing_key
            .sign(&response.encode_to_vec())
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
}

#[tonic::async_trait]
impl iot_config::Gateway for GatewayService {
    async fn location(
        &self,
        request: Request<GatewayLocationReqV1>,
    ) -> GrpcResult<GatewayLocationResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let address: &PublicKeyBinary = &request.gateway.into();

        let location = gateway_info::db::get_info(&self.metadata_pool, address)
            .await
            .map_err(|_| Status::internal("error fetching gateway info"))
            .and_then(|opt| {
                opt.ok_or_else(|| {
                    Status::not_found(format!("gateway not found: pubkey = {address}"))
                })
            })
            .and_then(|iot_metadata| {
                iot_metadata.location.ok_or_else(|| {
                    Status::not_found(format!("gateway unasserted: pubkey = {address}"))
                })
            })
            .and_then(|location| {
                Cell::from_raw(location).map_err(|_| {
                    Status::internal(format!(
                        "invalid h3 index location {location} for {address}"
                    ))
                })
            })
            .map(|cell| cell.to_string())?;

        let mut resp = GatewayLocationResV1 {
            location,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn region_params(
        &self,
        request: Request<GatewayRegionParamsReqV1>,
    ) -> GrpcResult<GatewayRegionParamsResV1> {
        let request = request.into_inner();

        let pubkey = self.verify_public_key(&request.address)?;
        request
            .verify(&pubkey)
            .map_err(|_| Status::permission_denied("invalid request signature"))?;

        let address: &PublicKeyBinary = &pubkey.into();
        tracing::debug!(pubkey = address.to_string(), "fetching region params");

        let default_region = Region::from_i32(request.region).ok_or(Status::invalid_argument(
            format!("invalid lora region {}", request.region),
        ))?;

        let (region, gain) = if let Some(info) =
            gateway_info::db::get_info(&self.metadata_pool, address)
                .await
                .map_err(|_| Status::internal("error fetching gateway info"))?
        {
            if let (Some(location), Some(gain)) = (info.location, info.gain) {
                let region = match hextree::Cell::from_raw(location) {
                    Ok(h3_location) => {
                        self.region_map.get_region(h3_location).unwrap_or_else(|| {
                            tracing::debug!(
                                pubkey = address.to_string(),
                                location = location,
                                "gateway region lookup failed for asserted location"
                            );
                            default_region
                        })
                    }
                    Err(_) => {
                        tracing::debug!(
                            pubkey = address.to_string(),
                            location = location,
                            "gateway asserted location is invalid h3 index"
                        );
                        default_region
                    }
                };
                (region, gain)
            } else {
                tracing::debug!(
                    pubkey = address.to_string(),
                    default_region = default_region.to_string(),
                    "gateway not asserted"
                );
                (default_region, 0)
            }
        } else {
            tracing::debug!(
                pubkey = address.to_string(),
                default_region = default_region.to_string(),
                "error retrieving gateway from chain"
            );
            (default_region, 0)
        };

        let params = self.region_map.get_params(&region);

        let mut resp = GatewayRegionParamsResV1 {
            region: region.into(),
            params,
            gain: gain as u64,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;
        tracing::debug!(
            pubkey = address.to_string(),
            region = region.to_string(),
            "returning region params"
        );
        Ok(Response::new(resp))
    }

    async fn info(&self, request: Request<GatewayInfoReqV1>) -> GrpcResult<GatewayInfoResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let address = &request.address.into();
        let metadata_info = gateway_info::db::get_info(&self.metadata_pool, address)
            .await
            .map_err(|_| Status::internal("error fetching gateway info"))?
            .ok_or_else(|| Status::not_found(format!("gateway not found: pubkey = {address:}")))?;

        let gateway_info = GatewayInfo::chain_metadata_to_info(metadata_info, &self.region_map);
        let mut resp = GatewayInfoResV1 {
            info: Some(gateway_info.try_into().map_err(|_| {
                Status::internal("unexpected error converting gateway info to protobuf")
            })?),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    type info_streamStream = GrpcStreamResult<GatewayInfoStreamResV1>;
    async fn info_stream(
        &self,
        request: Request<GatewayInfoStreamReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("fetching all gateways' info");

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;
        let region_map = self.region_map.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tokio::spawn(async move {
            stream_all_gateways_info(
                &pool,
                tx.clone(),
                &signing_key,
                region_map.clone(),
                batch_size,
            )
            .await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

async fn stream_all_gateways_info(
    pool: &Pool<Postgres>,
    tx: tokio::sync::mpsc::Sender<Result<GatewayInfoStreamResV1, Status>>,
    signing_key: &Keypair,
    region_map: RegionMapReader,
    batch_size: u32,
) -> anyhow::Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    let mut stream = gateway_info::db::all_info_stream(pool).chunks(batch_size as usize);
    while let Some(infos) = stream.next().await {
        let gateway_infos = infos
            .into_iter()
            .filter_map(|info| {
                GatewayInfo::chain_metadata_to_info(info, &region_map)
                    .try_into()
                    .ok()
            })
            .collect();

        let mut response = GatewayInfoStreamResV1 {
            gateways: gateway_infos,
            timestamp,
            signer: signer.clone(),
            signature: vec![],
        };

        response = match signing_key.sign(&response.encode_to_vec()) {
            Ok(signature) => GatewayInfoStreamResV1 {
                signature,
                ..response
            },
            Err(_) => {
                continue;
            }
        };

        tx.send(Ok(response)).await?;
    }
    Ok(())
}
