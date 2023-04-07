use crate::gateway_info;
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
    pub client: mobile_config::GatewayClient<Channel>,
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
impl gateway_info::GatewayInfoResolver for Client {
    type Error = ClientError;

    async fn resolve_gateway_info(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<gateway_info::GatewayInfo>, Self::Error> {
        let mut request = mobile_config::GatewayInfoReqV1 {
            address: address.clone().into(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(pubkey = address.to_string(), "fetching gateway info");
        let response = match self.client.info(request).await {
            Ok(info_res) => {
                let response = info_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response.info.map(gateway_info::GatewayInfo::from)
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => Err(status)?,
        };
        Ok(response)
    }

    async fn stream_gateways_info(
        &mut self,
    ) -> Result<gateway_info::GatewayInfoStream, Self::Error> {
        let mut req = mobile_config::GatewayInfoStreamReqV1 {
            batch_size: self.batch_size,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        tracing::debug!("fetching gateway info stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let res_stream = self
            .client
            .info_stream(req)
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
            .flat_map(|res| stream::iter(res.gateways.into_iter()))
            .map(gateway_info::GatewayInfo::from)
            .boxed();

        Ok(res_stream)
    }
}
