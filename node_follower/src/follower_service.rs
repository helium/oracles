use crate::{
    gateway_resp::{GatewayInfo, GatewayInfoResolver},
    Error, GatewayInfoStream, Result, CONNECT_TIMEOUT, DEFAULT_STREAM_BATCH_SIZE, DEFAULT_URI,
    RPC_TIMEOUT,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{
        self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1,
        FollowerGatewayStreamReqV1,
    },
    Channel, Endpoint,
};
use http::Uri;
use std::{env, str::FromStr};
use tonic::Streaming;

type FollowerClient = follower::Client<Channel>;

#[derive(Debug, Clone)]
pub struct FollowerService {
    pub client: FollowerClient,
    batch_size: u32,
}

#[async_trait::async_trait]
impl GatewayInfoResolver for FollowerService {
    async fn resolve_gateway_info(&mut self, address: &PublicKey) -> Result<GatewayInfo> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.client.find_gateway(req).await?.into_inner();
        match res.result {
            Some(GatewayResult::Info(gateway_info)) => Ok(gateway_info.try_into()?),
            _ => Err(Error::GatewayNotFound(format!("{address}"))),
        }
    }
}

impl FollowerService {
    pub fn from_env() -> Result<Self> {
        let uri = match env::var("FOLLOWER_URI") {
            Ok(var) => Uri::from_str(&var)?,
            Err(_) => Uri::from_static(DEFAULT_URI),
        };

        let batch_size = match env::var("GW_STREAM_BATCH_SIZE") {
            Ok(batch_size) => batch_size.parse()?,
            Err(_) => DEFAULT_STREAM_BATCH_SIZE,
        };

        Self::new(uri, batch_size)
    }

    pub fn new(uri: Uri, batch_size: u32) -> Result<Self> {
        let channel = Endpoint::from(uri)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: FollowerClient::new(channel),
            batch_size,
        })
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
        let res = self.client.txn_stream(req).await?.into_inner();
        Ok(res)
    }

    pub async fn active_gateways(&mut self) -> Result<GatewayInfoStream> {
        let req = FollowerGatewayStreamReqV1 {
            batch_size: self.batch_size,
        };
        let gw_stream = self
            .client
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
