use crate::gateway_info;
use file_store::traits::MsgVerify;
use futures::stream::{self, StreamExt};
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::{iot_config, Channel, Endpoint},
    BlockchainRegionParamV1, Message, Region,
};
use std::{sync::Arc, time::Duration};

pub mod org_client;
mod settings;

pub use org_client::OrgClient;
pub use settings::Settings;

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("error signing request: {0}")]
    Signing(#[from] helium_crypto::Error),
    #[error("grpc error response: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("error verifying response signature: {0}")]
    Verification(#[from] file_store::Error),
    #[error("error resolving region params: {0}")]
    UndefinedRegionParams(String),
}

#[derive(Clone, Debug)]
pub struct Client {
    pub gateway_client: iot_config::gateway_client::GatewayClient<Channel>,
    pub admin_client: iot_config::admin_client::AdminClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    batch_size: u32,
}

macro_rules! call_with_retry {
    ($rpc:expr) => {{
        use tonic::Code;

        let mut attempt = 1;
        loop {
            match $rpc.await {
                Ok(resp) => break Ok(resp),
                Err(status) => match status.code() {
                    Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable => {
                        if attempt < 3 {
                            attempt += 1;
                            tokio::time::sleep(Duration::from_secs(attempt)).await;
                            continue;
                        } else {
                            break Err(status);
                        }
                    }
                    _ => break Err(status),
                },
            }
        }
    }};
}

pub(crate) use call_with_retry;

impl Client {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let channel = Endpoint::from(settings.url.clone())
            .connect_timeout(Duration::from_secs(settings.connect_timeout))
            .timeout(Duration::from_secs(settings.rpc_timeout))
            .connect_lazy();
        Ok(Self {
            gateway_client: iot_config::gateway_client::GatewayClient::new(channel.clone()),
            admin_client: iot_config::admin_client::AdminClient::new(channel),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
            batch_size: settings.batch_size,
        })
    }

    pub async fn resolve_region_params(
        &mut self,
        region: Region,
    ) -> Result<RegionParamsInfo, ClientError> {
        let mut request = iot_config::RegionParamsReqV1 {
            region: region.into(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        let response =
            call_with_retry!(self.admin_client.region_params(request.clone()))?.into_inner();
        response.verify(&self.config_pubkey)?;
        Ok(RegionParamsInfo {
            region: response.region(),
            region_params: response
                .params
                .ok_or_else(|| ClientError::UndefinedRegionParams(format!("{region}")))?
                .region_params,
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
        let mut request = iot_config::GatewayInfoReqV1 {
            address: address.clone().into(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(pubkey = address.to_string(), "fetching gateway info");
        let response = match call_with_retry!(self.gateway_client.info(request.clone())) {
            Ok(info_resp) => {
                let response = info_resp.into_inner();
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
        let mut request = iot_config::GatewayInfoStreamReqV1 {
            batch_size: self.batch_size,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!("fetching gateway info stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let response_stream = call_with_retry!(self.gateway_client.info_stream(request.clone()))?
            .into_inner()
            .filter_map(|resp| async move { resp.ok() })
            .map(move |resp| (resp, pubkey.clone()))
            .filter_map(|(resp, pubkey)| async move { resp.verify(&pubkey).map(|_| resp).ok() })
            .flat_map(|resp| stream::iter(resp.gateways))
            .map(gateway_info::GatewayInfo::from)
            .boxed();

        Ok(response_stream)
    }
}

#[derive(Clone, Debug)]
pub struct RegionParamsInfo {
    pub region: Region,
    pub region_params: Vec<BlockchainRegionParamV1>,
}

#[derive(thiserror::Error, Debug)]
pub enum RegionParamsResolveError {
    #[error("params undefined for region: {0}")]
    Undefined(String),
}
