use crate::{
    admin::AuthCache,
    gateway_info::{self, GatewayInfo},
    org,
    region_map::RegionMapReader,
    telemetry, verify_public_key, GrpcResult, GrpcStreamResult, Settings,
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
use retainer::Cache;
use sqlx::{Pool, Postgres};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);
const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 3);

pub struct GatewayService {
    auth_cache: AuthCache,
    gateway_cache: Arc<Cache<PublicKeyBinary, GatewayInfo>>,
    metadata_pool: Pool<Postgres>,
    region_map: RegionMapReader,
    signing_key: Arc<Keypair>,
    delegate_cache: watch::Receiver<org::DelegateCache>,
}

impl GatewayService {
    pub fn new(
        settings: &Settings,
        metadata_pool: Pool<Postgres>,
        region_map: RegionMapReader,
        auth_cache: AuthCache,
        delegate_cache: watch::Receiver<org::DelegateCache>,
    ) -> Result<Self> {
        let gateway_cache = Arc::new(Cache::new());
        let cache_clone = gateway_cache.clone();
        tokio::spawn(async move { cache_clone.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });

        Ok(Self {
            auth_cache,
            gateway_cache,
            metadata_pool,
            region_map,
            signing_key: Arc::new(settings.signing_keypair()?),
            delegate_cache,
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

    fn verify_location_request(&self, request: &GatewayLocationReqV1) -> Result<(), Status> {
        let signature_bytes = request.signer.clone();
        self.delegate_cache
            .borrow()
            .contains(&signature_bytes.clone().into())
            .then(|| {
                let signer_pubkey = verify_public_key(&signature_bytes)?;
                request
                    .verify(&signer_pubkey)
                    .map_err(|_| Status::invalid_argument("bad request signature"))
            })
            .ok_or_else(|| Status::permission_denied("unauthorized request signature"))?
    }

    async fn resolve_gateway_info(&self, pubkey: &PublicKeyBinary) -> Result<GatewayInfo, Status> {
        match self.gateway_cache.get(pubkey).await {
            Some(gateway) => Ok(gateway.value().clone()),
            None => {
                let metadata = gateway_info::db::get_info(&self.metadata_pool, pubkey)
                    .await
                    .map_err(|_| Status::internal("error fetching gateway info"))?
                    .ok_or_else(|| {
                        telemetry::count_gateway_info_lookup("not-found");
                        Status::not_found(format!("gateway not found: pubkey = {pubkey:}"))
                    })?;
                let gateway = GatewayInfo::chain_metadata_to_info(metadata, &self.region_map);
                self.gateway_cache
                    .insert(pubkey.clone(), gateway.clone(), CACHE_TTL)
                    .await;
                if gateway.metadata.is_some() {
                    telemetry::count_gateway_info_lookup("asserted");
                } else {
                    telemetry::count_gateway_info_lookup("not-asserted");
                };
                Ok(gateway)
            }
        }
    }
}

#[tonic::async_trait]
impl iot_config::Gateway for GatewayService {
    async fn location(
        &self,
        request: Request<GatewayLocationReqV1>,
    ) -> GrpcResult<GatewayLocationResV1> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "location");

        self.verify_location_request(&request)?;

        let address: &PublicKeyBinary = &request.gateway.into();

        let location = self
            .resolve_gateway_info(address)
            .await
            .and_then(|gateway| match gateway.metadata {
                Some(metadata) => Ok(metadata.location),
                None => Err(Status::not_found(format!(
                    "gateway unasserted: pubkey = {address}"
                ))),
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
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn region_params(
        &self,
        request: Request<GatewayRegionParamsReqV1>,
    ) -> GrpcResult<GatewayRegionParamsResV1> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "region-params");
        let request_start = std::time::Instant::now();

        let pubkey = verify_public_key(&request.address)?;
        request
            .verify(&pubkey)
            .map_err(|_| Status::permission_denied("invalid request signature"))?;

        let address: &PublicKeyBinary = &pubkey.into();
        tracing::debug!(pubkey = %address, "fetching region params");

        let default_region = Region::from_i32(request.region).ok_or_else(|| {
            Status::invalid_argument(format!("invalid lora region {}", request.region))
        })?;

        let (region, gain) = match self.resolve_gateway_info(address).await {
            Err(_) => {
                tracing::debug!(
                    pubkey = %address,
                    %default_region,
                    "unable to retrieve gateway from chain"
                );
                (default_region, 0)
            }
            Ok(GatewayInfo { metadata, .. }) => match metadata {
                None => {
                    tracing::debug!(
                        pubkey = %address,
                        %default_region,
                        "gateway not asserted"
                    );
                    (default_region, 0)
                }
                Some(metadata) => (metadata.region, metadata.gain),
            },
        };
        telemetry::count_region_lookup(default_region, region);

        let params = self.region_map.get_params(&region);

        let mut resp = GatewayRegionParamsResV1 {
            region: region.into(),
            params,
            gain: gain as u64,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;
        tracing::debug!(
            pubkey = %address,
            %region,
            "returning region params"
        );
        telemetry::duration_gateway_info_lookup(request_start);
        Ok(Response::new(resp))
    }

    async fn info(&self, request: Request<GatewayInfoReqV1>) -> GrpcResult<GatewayInfoResV1> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let address = &request.address.into();
        let gateway_info = self.resolve_gateway_info(address).await?;

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
        telemetry::count_request("gateway", "info-stream");

        let signer = verify_public_key(&request.signer)?;
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
