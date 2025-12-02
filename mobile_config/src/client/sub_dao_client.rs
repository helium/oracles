use super::{call_with_retry, ClientError, Settings};
use crate::sub_dao_epoch_reward_info::EpochRewardInfo;
use helium_crypto::{Keypair, PublicKey};
use helium_proto::services::sub_dao::{self, SubDaoEpochRewardInfoReqV1};
use helium_proto_crypto::{MsgSign, MsgVerify};
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

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
            signing_key: settings.signing_keypair.clone(),
            config_pubkey: settings.config_pubkey.clone(),
        })
    }
}

#[async_trait::async_trait]
pub trait SubDaoEpochRewardInfoResolver: Clone + Send + Sync + 'static {
    async fn resolve_info(
        &self,
        sub_dao: &str,
        epoch: u64,
    ) -> Result<Option<EpochRewardInfo>, ClientError>;
}

#[async_trait::async_trait]
impl SubDaoEpochRewardInfoResolver for SubDaoClient {
    async fn resolve_info(
        &self,
        sub_dao: &str,
        epoch: u64,
    ) -> Result<Option<EpochRewardInfo>, ClientError> {
        let mut request = SubDaoEpochRewardInfoReqV1 {
            sub_dao_address: sub_dao.to_string(),
            epoch,
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        request.sign(&self.signing_key)?;
        tracing::debug!(
            subdao = sub_dao.to_string(),
            epoch = epoch,
            "fetching subdao epoch info"
        );
        let response = match call_with_retry!(self.client.clone().info(request.clone())) {
            Ok(info_res) => {
                let response = info_res.into_inner();
                response.verify(&self.config_pubkey)?;
                response.info.map(EpochRewardInfo::try_from).transpose()?
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            Err(status) => Err(status)?,
        };
        tracing::debug!(?response, "fetched subdao epoch info");
        Ok(response)
    }
}
