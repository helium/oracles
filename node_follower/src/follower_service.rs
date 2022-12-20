use crate::{
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
    settings::FollowerClients,
    Error, GatewayInfoStream, Result, Settings,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    follower::{
        self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1,
        FollowerGatewayStreamReqV1,
    },
    Channel,
};
use rand::{seq::SliceRandom, thread_rng};
use tonic::Streaming;

type FollowerClient = follower::Client<Channel>;

#[derive(Debug, Clone)]
pub struct FollowerService {
    pub clients: FollowerClients,
    batch_size: u32,
}

#[async_trait::async_trait]
impl GatewayInfoResolver for FollowerService {
    async fn resolve_gateway_info(&mut self, address: &PublicKeyBinary) -> Result<GatewayInfo> {
        let req = FollowerGatewayReqV1 {
            address: address.clone().into(),
        };
        let mut client = self.random_client()?;

        let res = client.find_gateway(req).await?.into_inner();
        match res.result {
            Some(GatewayResult::Info(gateway_info)) => Ok(gateway_info.try_into()?),
            _ => Err(Error::GatewayNotFound(format!("{address:?}"))),
        }
    }
}

impl FollowerService {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            clients: settings.connect_followers(),
            batch_size: settings.batch,
        }
    }

    pub fn random_client(&self) -> Result<FollowerClient> {
        let mut rng = thread_rng();
        match self.clients.choose(&mut rng).cloned() {
            Some(c) => Ok(c),
            None => Err(Error::ClientNotFound),
        }
    }

    pub async fn txn_stream<T>(
        &mut self,
        height: u64,
        txn_hash: &[u8],
        txn_types: &[T],
    ) -> Result<Streaming<follower::FollowerTxnStreamRespV1>>
    where
        T: ToString,
    {
        let req = follower::FollowerTxnStreamReqV1 {
            height,
            txn_hash: txn_hash.to_vec(),
            txn_types: txn_types.iter().map(|e| e.to_string()).collect(),
        };
        let mut client = self.random_client()?;
        let res = client.txn_stream(req).await?.into_inner();
        Ok(res)
    }

    pub async fn active_gateways(&mut self) -> Result<GatewayInfoStream> {
        let req = FollowerGatewayStreamReqV1 {
            batch_size: self.batch_size,
        };

        let mut client = self.random_client()?;

        let gw_stream = client
            .active_gateways(req)
            .await?
            .into_inner()
            .filter_map(|resp| async move { resp.ok() })
            .flat_map(|resp| stream::iter(resp.gateways.into_iter()))
            .filter_map(|resp| async move {
                match resp.result {
                    Some(GatewayResult::Info(gateway_info)) => {
                        GatewayInfo::try_from(gateway_info).ok()
                    }
                    _ => None,
                }
            })
            .boxed();

        Ok(gw_stream)
    }
}
