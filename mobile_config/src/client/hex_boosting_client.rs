use super::{call_with_retry, ClientError, Settings};
use crate::boosted_hex_info::{self, BoostedHexInfoStream};
use chrono::{DateTime, Utc};
use file_store::traits::MsgVerify;
use futures::stream::{self, StreamExt};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::{mobile_config, Channel},
    Message,
};
use std::{error::Error, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct HexBoostingClient {
    pub client: mobile_config::HexBoostingClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
    batch_size: u32,
}

impl HexBoostingClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        Ok(Self {
            client: settings.connect_hex_boosting_service_client(),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
            batch_size: settings.hex_boosting_batch_size,
        })
    }
}

#[async_trait::async_trait]
pub trait HexBoostingInfoResolver: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn stream_boosted_hexes_info(&mut self) -> Result<BoostedHexInfoStream, Self::Error>;

    async fn stream_modified_boosted_hexes_info(
        &mut self,
        timestamp: DateTime<Utc>,
    ) -> Result<BoostedHexInfoStream, Self::Error>;
}

#[async_trait::async_trait]
impl HexBoostingInfoResolver for HexBoostingClient {
    type Error = ClientError;

    async fn stream_boosted_hexes_info(&mut self) -> Result<BoostedHexInfoStream, Self::Error> {
        let mut req = mobile_config::BoostedHexInfoStreamReqV1 {
            batch_size: self.batch_size,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        tracing::debug!("fetching boosted hexes info stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let res_stream = call_with_retry!(self.client.info_stream(req.clone()))?
            .into_inner()
            .filter_map(|res| async move { res.ok() })
            .map(move |res| (res, pubkey.clone()))
            .filter_map(|(res, pubkey)| async move {
                match res.verify(&pubkey) {
                    Ok(()) => Some(res),
                    Err(_) => None,
                }
            })
            .flat_map(|res| stream::iter(res.hexes))
            .map(boosted_hex_info::BoostedHexInfo::try_from)
            .filter_map(|hex| async move { hex.ok() })
            .boxed();

        Ok(res_stream)
    }

    async fn stream_modified_boosted_hexes_info(
        &mut self,
        timestamp: DateTime<Utc>,
    ) -> Result<BoostedHexInfoStream, Self::Error> {
        let mut req = mobile_config::BoostedHexModifiedInfoStreamReqV1 {
            batch_size: self.batch_size,
            timestamp: timestamp.timestamp() as u64,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        tracing::debug!("fetching modified boosted hexes info stream");
        let pubkey = Arc::new(self.config_pubkey.clone());
        let res_stream = call_with_retry!(self.client.modified_info_stream(req.clone()))?
            .into_inner()
            .filter_map(|res| async move { res.ok() })
            .map(move |res| (res, pubkey.clone()))
            .filter_map(|(res, pubkey)| async move {
                match res.verify(&pubkey) {
                    Ok(()) => Some(res),
                    Err(_) => None,
                }
            })
            .flat_map(|res| stream::iter(res.hexes))
            .map(boosted_hex_info::BoostedHexInfo::try_from)
            .filter_map(|hex| async move { hex.ok() })
            .boxed();

        Ok(res_stream)
    }
}
