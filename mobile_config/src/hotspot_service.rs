use crate::{
    hotspot_metadata::{self, HotspotMetadata},
    key_cache::{KeyCache, KeyType},
    GrpcResult, GrpcStreamResult,
};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::{
    stream::{StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::{
    services::mobile_config::{
        self, HotspotMetadataReqV1, HotspotMetadataResV1, HotspotMetadataStreamReqV1,
        HotspotMetadataStreamResV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct HotspotService {
    key_cache: KeyCache,
    metadata_pool: Pool<Postgres>,
    signing_key: Arc<Keypair>,
}

impl HotspotService {
    pub fn new(key_cache: KeyCache, metadata_pool: Pool<Postgres>, signing_key: Keypair) -> Self {
        Self {
            key_cache,
            metadata_pool,
            signing_key: Arc::new(signing_key),
        }
    }

    async fn verify_request_signature<'a, R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .key_cache
            .verify_signature(KeyType::Oracle, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by oracle");
            return Ok(());
        } else if self
            .key_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn sign_response<R>(&self, response: &R) -> Result<Vec<u8>, Status>
    where
        R: Message,
    {
        self.signing_key
            .sign(&response.encode_to_vec())
            .map_err(|_| Status::internal("response signing error"))
    }
}

#[tonic::async_trait]
impl mobile_config::Hotspot for HotspotService {
    async fn metadata(
        &self,
        request: Request<HotspotMetadataReqV1>,
    ) -> GrpcResult<HotspotMetadataResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let pubkey: PublicKeyBinary = request.address.into();
        tracing::debug!(pubkey = pubkey.to_string(), "fetching hotspot metadata");

        hotspot_metadata::get_metadata(&self.metadata_pool, &pubkey)
            .await
            .map_err(|_| Status::internal("error fetching hotspot metadata"))?
            .map_or_else(
                || Err(Status::not_found(pubkey.to_string())),
                |metadata| {
                    let metadata = metadata
                        .try_into()
                        .map_err(|_| Status::internal("error serializing hotspot metadata"))?;
                    let mut res = HotspotMetadataResV1 {
                        metadata: Some(metadata),
                        timestamp: Utc::now().encode_timestamp(),
                        signature: vec![],
                    };
                    res.signature = self.sign_response(&res)?;
                    Ok(Response::new(res))
                },
            )
    }

    type metadata_streamStream = GrpcStreamResult<HotspotMetadataStreamResV1>;
    async fn metadata_stream(
        &self,
        request: Request<HotspotMetadataStreamReqV1>,
    ) -> GrpcResult<Self::metadata_streamStream> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        tracing::debug!("fetching all hotspots' metadata");

        let pool = self.metadata_pool.clone();
        let signing_key = self.signing_key.clone();
        let batch_size = request.batch_size;

        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tokio::spawn(async move {
            stream_all_hotspots_metadata(&pool, tx.clone(), signing_key.clone(), batch_size).await
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

async fn stream_all_hotspots_metadata(
    pool: &Pool<Postgres>,
    tx: tokio::sync::mpsc::Sender<Result<HotspotMetadataStreamResV1, Status>>,
    signing_key: Arc<Keypair>,
    batch_size: u32,
) -> anyhow::Result<()> {
    Ok(hotspot_metadata::all_metadata_stream(pool)
        .map(Ok::<HotspotMetadata, sqlx::Error>)
        .try_filter_map(|metadata| async move {
            let result: Option<mobile_config::HotspotMetadata> = match metadata.try_into() {
                Ok(metadata_proto) => Some(metadata_proto),
                Err(_) => None,
            };
            Ok(result)
        })
        .try_chunks(batch_size as usize)
        .map_ok(move |batch| {
            (
                HotspotMetadataStreamResV1 {
                    hotspots: batch,
                    timestamp: Utc::now().encode_timestamp(),
                    signature: vec![],
                },
                signing_key.clone(),
            )
        })
        .try_filter_map(|(res, keypair)| async move {
            let result = match keypair.sign(&res.encode_to_vec()) {
                Ok(signature) => Some(HotspotMetadataStreamResV1 {
                    hotspots: res.hotspots,
                    timestamp: res.timestamp,
                    signature,
                }),
                Err(_) => None,
            };
            Ok(result)
        })
        .map_err(|err| Status::internal(format!("metadata batch failed with reason: {err:?}")))
        .try_for_each(|res| {
            tx.send(Ok(res))
                .map_err(|err| Status::internal(format!("metadata batch send failed {err:?}")))
        })
        .or_else(|err| {
            tx.send(Err(Status::internal(format!(
                "metadata batch failed with reason: {err:?}"
            ))))
        })
        .await?)
}
