use crate::hotspot_metadata;
use file_store::traits::MsgVerify;
use futures::stream::{self, StreamExt};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::{mobile_config, Channel},
    Message,
};
use std::sync::Arc;

mod settings;
pub use settings::Settings;

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("error signing request {0}")]
    SigningError(#[from] helium_crypto::Error),
    #[error("grpc error response {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("error verifying response signature {0}")]
    VerificationError(#[from] file_store::Error),
}

#[derive(Clone, Debug)]
pub struct Client {
    pub client: mobile_config::HotspotClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    batch_size: u32,
}

impl Client {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        Ok(Self {
            client: settings.connect(),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
            batch_size: settings.batch_size,
        })
    }
}

#[async_trait::async_trait]
impl hotspot_metadata::HotspotMetadataResolver for Client {
    type Error = ClientError;

    async fn resolve_hotspot_metadata(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<hotspot_metadata::HotspotMetadata>, Self::Error> {
        let mut request = mobile_config::HotspotMetadataReqV1 {
            address: address.clone().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(pubkey = address.to_string(), "fetching hotspot metadata");
        let response = match self.client.metadata(request).await {
            Ok(metadata_res) => {
                let response = metadata_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response
                    .metadata
                    .map(hotspot_metadata::HotspotMetadata::from)
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => Err(status)?,
        };
        Ok(response)
    }

    async fn stream_hotspots_metadata(
        &mut self,
    ) -> Result<hotspot_metadata::HotspotMetadataStream, Self::Error> {
        let mut req = mobile_config::HotspotMetadataStreamReqV1 {
            batch_size: self.batch_size,
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        tracing::debug!("fetching hotspot metadata stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let res_stream = self
            .client
            .metadata_stream(req)
            .await?
            .into_inner()
            .filter_map(|res| async move { res.ok() })
            .map(move |res| (res, pubkey.clone()))
            .filter_map(|(res, pubkey)| async move {
                match res.verify(&pubkey) {
                    Ok(()) => Some(res),
                    Err(_) => None,
                }
            })
            .flat_map(|res| stream::iter(res.hotspots.into_iter()))
            .map(hotspot_metadata::HotspotMetadata::from)
            .boxed();

        Ok(res_stream)
    }
}
