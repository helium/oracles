use crate::{
    gateway_info::{
        self,
        db::{get_batch_tracked_radios, get_updated_radios},
        DeviceType, GatewayInfo,
    },
    gateway_info_v3::{self, DeviceTypeV2},
    key_cache::KeyCache,
    telemetry, verify_public_key, GrpcResult, GrpcStreamResult,
};
use chrono::{DateTime, TimeZone, Utc};
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::{
    future,
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::mobile_config::{
        self, GatewayInfoBatchReqV1, GatewayInfoReqV1, GatewayInfoResV1, GatewayInfoResV2,
        GatewayInfoStreamReqV1, GatewayInfoStreamReqV2, GatewayInfoStreamReqV3,
        GatewayInfoStreamResV1, GatewayInfoStreamResV2, GatewayInfoStreamResV3, GatewayInfoV2,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use std::{collections::HashMap, sync::Arc};
use tonic::{Request, Response, Status};

pub struct GatewayService {
    key_cache: KeyCache,
    mobile_config_db_pool: Pool<Postgres>,
    metadata_pool: Pool<Postgres>,
    signing_key: Arc<Keypair>,
}

impl GatewayService {
    pub fn new(
        key_cache: KeyCache,
        metadata_pool: Pool<Postgres>,
        signing_key: Keypair,
        mobile_config_db_pool: Pool<Postgres>,
    ) -> Self {
        Self {
            key_cache,
            metadata_pool,
            signing_key: Arc::new(signing_key),
            mobile_config_db_pool,
        }
    }

    fn verify_request_signature<R>(&self, signer: &PublicKey, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self.key_cache.verify_signature(signer, request).is_ok() {
            tracing::debug!(signer = signer.to_string(), "request authorized");
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn verify_request_signature_for_info(&self, request: &GatewayInfoReqV1) -> Result<(), Status> {
        let signer = verify_public_key(&request.signer)?;
        let address = verify_public_key(&request.address)?;

        if address == signer && request.verify(&signer).is_ok() {
            tracing::debug!(%signer, "self authorized");
            return Ok(());
        }

        self.verify_request_signature(&signer, request)
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }
}

#[tonic::async_trait]
impl mobile_config::Gateway for GatewayService {
    // Deprecated
    async fn info(&self, request: Request<GatewayInfoReqV1>) -> GrpcResult<GatewayInfoResV1> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info");
        custom_tracing::record_b58("pub_key", &request.address);
        custom_tracing::record_b58("signer", &request.signer);

        self.verify_request_signature_for_info(&request)?;

        let pubkey: PublicKeyBinary = request.address.into();
        tracing::debug!(pubkey = pubkey.to_string(), "fetching gateway info");

        gateway_info::db::get_info(&self.metadata_pool, &pubkey)
            .await
            .map_err(|_| Status::internal("error fetching gateway info"))?
            .map_or_else(
                || {
                    telemetry::count_gateway_chain_lookup("not-found");
                    Err(Status::not_found(pubkey.to_string()))
                },
                |info| {
                    if info.metadata.is_some() {
                        telemetry::count_gateway_chain_lookup("asserted");
                    } else {
                        telemetry::count_gateway_chain_lookup("not-asserted");
                    };
                    let info = info
                        .try_into()
                        .map_err(|_| Status::internal("error serializing gateway info"))?;
                    let mut res = GatewayInfoResV1 {
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

    async fn info_v2(&self, request: Request<GatewayInfoReqV1>) -> GrpcResult<GatewayInfoResV2> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-v2");
        custom_tracing::record_b58("pub_key", &request.address);
        custom_tracing::record_b58("signer", &request.signer);

        self.verify_request_signature_for_info(&request)?;

        let pubkey: PublicKeyBinary = request.address.into();
        tracing::debug!(pubkey = pubkey.to_string(), "fetching gateway info (v2)");

        let updated_at = gateway_info::db::get_updated_at(&self.mobile_config_db_pool, &pubkey)
            .await
            .map_err(|_| {
                Status::internal("error fetching updated_at field for gateway info (v2)")
            })?;

        gateway_info::db::get_info(&self.metadata_pool, &pubkey)
            .await
            .map_err(|_| Status::internal("error fetching gateway info (v2)"))?
            .map_or_else(
                || {
                    telemetry::count_gateway_chain_lookup("not-found");
                    Err(Status::not_found(pubkey.to_string()))
                },
                |mut info| {
                    if info.metadata.is_some() {
                        telemetry::count_gateway_chain_lookup("asserted");
                    } else {
                        telemetry::count_gateway_chain_lookup("not-asserted");
                    };

                    // determine updated_at
                    if let Some(v) = updated_at {
                        info.updated_at = Some(v)
                    } else if info.refreshed_at.is_some() {
                        info.updated_at = info.refreshed_at;
                    } else {
                        info.updated_at = info.created_at;
                    }

                    let info: GatewayInfoV2 = info
                        .try_into()
                        .map_err(|_| Status::internal("error serializing gateway info (v2)"))?;

                    let mut res = GatewayInfoResV2 {
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

    // Deprecated
    type info_batchStream = GrpcStreamResult<GatewayInfoStreamResV1>;
    async fn info_batch(
        &self,
        request: Request<GatewayInfoBatchReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-batch");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!(
            batch = request.addresses.len(),
            "fetching gateways' info batch"
        );

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;
        let addresses = request
            .addresses
            .into_iter()
            .map(|key| key.into())
            .collect::<Vec<PublicKeyBinary>>();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let stream = gateway_info::db::batch_info_stream(&pool, &addresses)?;
            stream_multi_gateways_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type info_batch_v2Stream = GrpcStreamResult<GatewayInfoStreamResV2>;
    async fn info_batch_v2(
        &self,
        request: Request<GatewayInfoBatchReqV1>,
    ) -> GrpcResult<Self::info_batch_v2Stream> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-batch-v2");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!(
            batch = request.addresses.len(),
            "fetching gateways' info batch"
        );

        let metadata_db_pool = self.metadata_pool.clone();
        let mobile_config_db_pool = self.mobile_config_db_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;
        let addresses = request
            .addresses
            .into_iter()
            .map(|key| key.into())
            .collect::<Vec<PublicKeyBinary>>();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let min_updated_at = DateTime::UNIX_EPOCH;
            let updated_radios =
                get_batch_tracked_radios(&mobile_config_db_pool, &addresses).await?;

            let stream = gateway_info::db::batch_info_stream(&metadata_db_pool, &addresses)?;
            let stream = stream
                .filter_map(|gateway_info| {
                    future::ready(handle_updated_at(
                        gateway_info,
                        &updated_radios,
                        min_updated_at,
                    ))
                })
                .boxed();
            stream_multi_gateways_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    // Deprecated
    type info_streamStream = GrpcStreamResult<GatewayInfoStreamResV1>;
    async fn info_stream(
        &self,
        request: Request<GatewayInfoStreamReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-stream");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let device_types: Vec<DeviceType> = request.device_types().map(|v| v.into()).collect();
        tracing::debug!(
            "fetching all gateways' info. Device types: {:?} ",
            device_types
        );

        tokio::spawn(async move {
            let stream = gateway_info::db::all_info_stream(&pool, &device_types);
            stream_multi_gateways_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type info_stream_v2Stream = GrpcStreamResult<GatewayInfoStreamResV2>;
    async fn info_stream_v2(
        &self,
        request: Request<GatewayInfoStreamReqV2>,
    ) -> GrpcResult<Self::info_stream_v2Stream> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-stream-v2");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let metadata_db_pool = self.metadata_pool.clone();
        let mobile_config_db_pool = self.mobile_config_db_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let device_types: Vec<DeviceType> = request.device_types().map(|v| v.into()).collect();

        tracing::debug!(
            "fetching all gateways' info (v2). Device types: {:?} ",
            device_types
        );

        tokio::spawn(async move {
            let min_updated_at = Utc
                .timestamp_opt(request.min_updated_at as i64, 0)
                .single()
                .ok_or(Status::invalid_argument(
                    "Invalid min_refreshed_at argument",
                ))?;

            let updated_radios = get_updated_radios(&mobile_config_db_pool, min_updated_at).await?;
            let stream = gateway_info::db::all_info_stream(&metadata_db_pool, &device_types);
            let stream = stream
                .filter_map(|gateway_info| {
                    future::ready(handle_updated_at(
                        gateway_info,
                        &updated_radios,
                        min_updated_at,
                    ))
                })
                .boxed();
            stream_multi_gateways_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type info_stream_v3Stream = GrpcStreamResult<GatewayInfoStreamResV3>;
    async fn info_stream_v3(
        &self,
        request: Request<GatewayInfoStreamReqV3>,
    ) -> GrpcResult<Self::info_stream_v3Stream> {
        let request = request.into_inner();
        telemetry::count_request("gateway", "info-stream-v3");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let metadata_db_pool = self.metadata_pool.clone();
        let mobile_config_db_pool = self.mobile_config_db_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let device_types: Vec<DeviceTypeV2> = request.device_types().map(|v| v.into()).collect();

        tokio::spawn(async move {
            let min_updated_at = Utc
                .timestamp_opt(request.min_updated_at as i64, 0)
                .single()
                .ok_or(Status::invalid_argument(
                    "Invalid min_refreshed_at argument",
                ))
                .unwrap(); // TODO

            let _min_location_changed_at = Utc
                .timestamp_opt(request.min_location_changed_at as i64, 0)
                .single()
                .ok_or(Status::invalid_argument(
                    "Invalid min_location_changed_at argument",
                ))
                .unwrap(); // TODO;

            let _updated_radios =
                get_updated_radios(&mobile_config_db_pool, min_updated_at).await?;
            let stream = gateway_info_v3::db::all_info_stream_v3(&metadata_db_pool, &device_types);
            // let stream = stream
            //     .filter_map(|gateway_info| {
            //         // todo set location and location_changed_at here?
            //         future::ready(handle_updated_at(
            //             gateway_info,
            //             &updated_radios,
            //             min_updated_at,
            //         ))
            //     })
            //     .boxed();
            stream_multi_gateways_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

fn handle_updated_at(
    mut gateway_info: GatewayInfo,
    updated_radios: &HashMap<PublicKeyBinary, chrono::DateTime<Utc>>,
    min_updated_at: chrono::DateTime<Utc>,
) -> Option<GatewayInfo> {
    // Check mobile_radio_tracker HashMap
    if let Some(updated_at) = updated_radios.get(&gateway_info.address) {
        // It could be already filtered by min_updated_at but recheck won't hurt
        if updated_at >= &min_updated_at {
            gateway_info.updated_at = Some(*updated_at);
            return Some(gateway_info);
        }
        return None;
    }
    // Fallback solution #1. Try to use refreshed_at as updated_at field and check
    // min_updated_at
    if let Some(refreshed_at) = gateway_info.refreshed_at {
        if refreshed_at >= min_updated_at {
            gateway_info.updated_at = Some(refreshed_at);
            return Some(gateway_info);
        }
        return None;
    }
    // Fallback solution #2. Try to use created_at as updated_at field and check
    // min_updated_at
    if let Some(created_at) = gateway_info.created_at {
        if created_at >= min_updated_at {
            gateway_info.updated_at = Some(created_at);
            return Some(gateway_info);
        }
        return None;
    }
    None
}

trait GatewayInfoStreamRes {
    type GatewayInfoType;
    fn new(gateways: Vec<Self::GatewayInfoType>, timestamp: u64, signer: Vec<u8>) -> Self;
    fn set_signature(&mut self, signature: Vec<u8>);
}

impl GatewayInfoStreamRes for GatewayInfoStreamResV1 {
    type GatewayInfoType = mobile_config::GatewayInfo;

    fn new(gateways: Vec<Self::GatewayInfoType>, timestamp: u64, signer: Vec<u8>) -> Self {
        GatewayInfoStreamResV1 {
            gateways,
            timestamp,
            signer,
            signature: vec![],
        }
    }

    fn set_signature(&mut self, signature: Vec<u8>) {
        self.signature = signature;
    }
}

impl GatewayInfoStreamRes for GatewayInfoStreamResV2 {
    type GatewayInfoType = mobile_config::GatewayInfoV2;

    fn new(gateways: Vec<Self::GatewayInfoType>, timestamp: u64, signer: Vec<u8>) -> Self {
        GatewayInfoStreamResV2 {
            gateways,
            timestamp,
            signer,
            signature: vec![],
        }
    }

    fn set_signature(&mut self, signature: Vec<u8>) {
        self.signature = signature;
    }
}

impl GatewayInfoStreamRes for GatewayInfoStreamResV3 {
    type GatewayInfoType = mobile_config::GatewayInfoV3;

    fn new(gateways: Vec<Self::GatewayInfoType>, timestamp: u64, signer: Vec<u8>) -> Self {
        GatewayInfoStreamResV3 {
            gateways,
            timestamp,
            signer,
            signature: vec![],
        }
    }

    fn set_signature(&mut self, signature: Vec<u8>) {
        self.signature = signature;
    }
}

async fn stream_multi_gateways_info<T, GI>(
    stream: impl Stream<Item = GI>,
    tx: tokio::sync::mpsc::Sender<Result<T, Status>>,
    signing_key: Arc<Keypair>,
    batch_size: u32,
) -> anyhow::Result<()>
where
    T: GatewayInfoStreamRes + Send + Sync + 'static + helium_proto::Message,
    T::GatewayInfoType: TryFrom<GI> + Send + 'static,
{
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    Ok(stream
        .map(Ok::<GI, sqlx::Error>)
        .try_filter_map(|info| async move {
            let result: Option<T::GatewayInfoType> = info.try_into().ok();
            Ok(result)
        })
        .try_chunks(batch_size as usize)
        .map_ok(move |batch| {
            (
                T::new(batch, timestamp, signer.clone()),
                signing_key.clone(),
            )
        })
        .try_filter_map(|(mut res, keypair)| async move {
            let result = match keypair.sign(&helium_proto::Message::encode_to_vec(&res)) {
                Ok(signature) => {
                    res.set_signature(signature);
                    Some(res)
                }
                Err(_) => None,
            };
            Ok(result)
        })
        .map_err(|err| Status::internal(format!("info batch failed with reason: {err:?}")))
        .try_for_each(|res| {
            tx.send(Ok(res))
                .map_err(|err| Status::internal(format!("info batch send failed {err:?}")))
        })
        .or_else(|err| {
            tx.send(Err(Status::internal(format!(
                "info batch failed with reason: {err:?}"
            ))))
        })
        .await?)
}
