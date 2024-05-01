use crate::{
    boosted_hex_info::{self, BoostedHexInfo},
    key_cache::KeyCache,
    telemetry, verify_public_key, GrpcResult, GrpcStreamResult,
};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampDecode, TimestampEncode};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::mobile_config::{
        self, BoostedHexInfoStreamReqV1, BoostedHexInfoStreamResV1,
        BoostedHexModifiedInfoStreamReqV1,
    },
    BoostedHexInfoV1, Message,
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct HexBoostingService {
    key_cache: KeyCache,
    metadata_pool: Pool<Postgres>,
    signing_key: Arc<Keypair>,
}

impl HexBoostingService {
    pub fn new(key_cache: KeyCache, metadata_pool: Pool<Postgres>, signing_key: Keypair) -> Self {
        Self {
            key_cache,
            metadata_pool,
            signing_key: Arc::new(signing_key),
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
}

#[tonic::async_trait]
impl mobile_config::HexBoosting for HexBoostingService {
    type info_streamStream = GrpcStreamResult<BoostedHexInfoStreamResV1>;
    async fn info_stream(
        &self,
        request: Request<BoostedHexInfoStreamReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let request = request.into_inner();
        telemetry::count_request("hex-boosting", "info-stream");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("fetching all boosted hexes' info");

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let stream = boosted_hex_info::db::all_info_stream_with_time_now(&pool);
            stream_multi_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type modified_info_streamStream = GrpcStreamResult<BoostedHexInfoStreamResV1>;
    async fn modified_info_stream(
        &self,
        request: Request<BoostedHexModifiedInfoStreamReqV1>,
    ) -> GrpcResult<Self::info_streamStream> {
        let request = request.into_inner();
        telemetry::count_request("hex-boosting", "modified-info-stream");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        tracing::debug!("fetching all modified boosted hexes' info");

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;
        let ts = request
            .timestamp
            .to_timestamp()
            .map_err(|_| Status::invalid_argument("invalid timestamp"))
            .unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let stream = boosted_hex_info::db::modified_info_stream(&pool, ts);
            stream_multi_info(stream, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

async fn stream_multi_info(
    stream: impl Stream<Item = BoostedHexInfo>,
    tx: tokio::sync::mpsc::Sender<Result<BoostedHexInfoStreamResV1, Status>>,
    signing_key: Arc<Keypair>,
    batch_size: u32,
) -> anyhow::Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    Ok(stream
        .map(Ok::<BoostedHexInfo, sqlx::Error>)
        .try_filter_map(|info| async move {
            let result: Option<BoostedHexInfoV1> = info.try_into().ok();
            Ok(result)
        })
        .try_chunks(batch_size as usize)
        .map_ok(move |batch| {
            (
                BoostedHexInfoStreamResV1 {
                    hexes: batch,
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                },
                signing_key.clone(),
            )
        })
        .try_filter_map(|(res, keypair)| async move {
            let result = match keypair.sign(&res.encode_to_vec()) {
                Ok(signature) => Some(BoostedHexInfoStreamResV1 {
                    hexes: res.hexes,
                    timestamp: res.timestamp,
                    signer: res.signer,
                    signature,
                }),
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
