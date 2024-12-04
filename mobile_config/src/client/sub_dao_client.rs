use super::{call_with_retry, ClientError, Settings};
use crate::sub_dao_epoch_reward_info::ResolvedSubDaoEpochRewardInfo;
use file_store::traits::MsgVerify;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::{
        sub_dao::{self, SubDaoEpochRewardInfoReqV1},
        Channel,
    },
    Message,
};
use std::{error::Error, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct SubDaoClient {
    pub client: sub_dao::sub_dao_client::SubDaoClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
}

impl SubDaoClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        Ok(Self {
            client: settings.connect_epoch_client(),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
        })
    }
}

#[async_trait::async_trait]
pub trait SubDaoEpochRewardInfoResolver: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn resolve_info(
        &self,
        sub_dao: &str,
        epoch: u64,
    ) -> Result<Option<ResolvedSubDaoEpochRewardInfo>, Self::Error>;
}

#[async_trait::async_trait]
impl SubDaoEpochRewardInfoResolver for SubDaoClient {
    type Error = ClientError;

    async fn resolve_info(
        &self,
        sub_dao: &str,
        epoch: u64,
    ) -> Result<Option<ResolvedSubDaoEpochRewardInfo>, Self::Error> {
        let mut request = SubDaoEpochRewardInfoReqV1 {
            sub_dao_address: sub_dao.into(),
            epoch,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.signature = self.signing_key.sign(&request.encode_to_vec())?;
        tracing::debug!(
            subdao = sub_dao.to_string(),
            epoch = epoch,
            "fetching subdao epoch info"
        );
        let response = match call_with_retry!(self.client.clone().info(request.clone())) {
            Ok(info_res) => {
                let response = info_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response
                    .info
                    .map(ResolvedSubDaoEpochRewardInfo::try_from)
                    .transpose()?
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => Err(status)?,
        };
        Ok(response)
    }
}
