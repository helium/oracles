use crate::{
    admin::AuthCache,
    gateway_info::{self, GatewayInfo},
    org,
    region_map::RegionMapReader,
    telemetry, verify_public_key, GrpcResult, GrpcStreamResult, Settings,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::TimestampEncode;
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
use helium_proto_crypto::MsgVerify;
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
        let signer_pubkey = verify_public_key(&signature_bytes)?;

        if self
            .auth_cache
            .verify_signature(&signer_pubkey, request)
            .is_ok()
        {
            return Ok(());
        }

        self.delegate_cache
            .borrow()
            .contains(&signature_bytes.clone().into())
            .then(|| {
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
                let metadata = tokio::select! {
                    query_result = gateway_info::db::get_info(&self.metadata_pool, pubkey) => {
                        query_result.map_err(|_| Status::internal("error fetching gateway info"))?
                            .ok_or_else(|| {
                                telemetry::count_gateway_info_lookup("not-found");
                                Status::not_found(format!("gateway not found: pubkey = {pubkey:}"))
                            })?
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(3)) => {
                        tracing::warn!("gateway info request query timed out");
                        return Err(Status::deadline_exceeded("query timed out requesting info"))
                    }
                };
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
        custom_tracing::record_b58("pub_key", &request.gateway);
        custom_tracing::record_b58("signer", &request.signer);

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
        custom_tracing::record_b58("pub_key", &request.address);
        let request_start = std::time::Instant::now();

        let pubkey = verify_public_key(&request.address)?;
        request
            .verify(&pubkey)
            .map_err(|_| Status::permission_denied("invalid request signature"))?;

        let address: &PublicKeyBinary = &pubkey.into();
        tracing::debug!(pubkey = %address, "fetching region params");

        let default_region = Region::try_from(request.region).map_err(|_| {
            Status::invalid_argument(format!("invalid lora region {}", request.region))
        })?;

        let (region, gain) = match self.resolve_gateway_info(address).await {
            Err(status) if status.code() == tonic::Code::NotFound => {
                tracing::debug!(
                    pubkey = %address,
                    %default_region,
                    "unable to retrieve gateway from chain"
                );
                (default_region, 0)
            }
            Err(status) => {
                tracing::error!(
                    pubkey = %address,
                    %default_region,
                    "gateway lookup failed"
                );
                telemetry::count_region_lookup(default_region, None);
                return Err(Status::internal(status.message().to_string()));
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
        telemetry::count_region_lookup(default_region, Some(region));

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
        custom_tracing::record_b58("pub_key", &request.address);
        custom_tracing::record_b58("signer", &request.signer);

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
            tokio::select! {
                _ = stream_all_gateways_info(
                    &pool,
                    tx.clone(),
                    &signing_key,
                    region_map.clone(),
                    batch_size,
                ) => (),
            }
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

        let mut gateway = GatewayInfoStreamResV1 {
            gateways: gateway_infos,
            timestamp,
            signer: signer.clone(),
            signature: vec![],
        };

        gateway = match signing_key.sign(&gateway.encode_to_vec()) {
            Ok(signature) => GatewayInfoStreamResV1 {
                signature,
                ..gateway
            },
            Err(_) => {
                continue;
            }
        };

        tx.send(Ok(gateway)).await?;
    }
    Ok(())
}
