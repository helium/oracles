use crate::{
    subnetwork_rewards::RewardPeriod, token_type::BlockchainTokenTypeV1, traits::OwnerResolver,
    Result,
};
use async_trait::async_trait;
use helium_crypto::PublicKey;
use helium_proto::{
    services::{
        follower::{
            self, FollowerGatewayReqV1, FollowerSubnetworkLastRewardHeightReqV1,
            FollowerTxnStreamReqV1, FollowerTxnStreamRespV1,
        },
        Channel, Endpoint,
    },
    BlockchainTokenTypeV1 as ProtoTokenType,
};
use http::Uri;
use std::{env, str::FromStr, time::Duration};
use tonic::Streaming;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

type FollowerClient = follower::Client<Channel>;

#[derive(Debug, Clone)]
pub struct FollowerService {
    client: FollowerClient,
}

#[async_trait]
impl OwnerResolver for FollowerService {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.client.find_gateway(req).await?.into_inner();

        if let Ok(pub_key) = PublicKey::try_from(res.owner) {
            return Ok(Some(pub_key));
        }

        Ok(None)
    }
}

impl FollowerService {
    pub fn from_env() -> Result<Self> {
        let uri =
            Uri::from_str(&env::var("FOLLOWER_URI").unwrap_or_else(|_| DEFAULT_URI.to_string()))?;
        Self::new(uri)
    }

    pub fn new(uri: Uri) -> Result<Self> {
        let channel = Endpoint::from(uri)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: FollowerClient::new(channel),
        })
    }

    pub async fn txn_stream<T>(
        &mut self,
        height: u64,
        txn_hash: &[u8],
        txn_types: &[T],
    ) -> Result<Streaming<FollowerTxnStreamRespV1>>
    where
        T: ToString,
    {
        let req = FollowerTxnStreamReqV1 {
            height,
            txn_hash: txn_hash.to_vec(),
            txn_types: txn_types.iter().map(|e| e.to_string()).collect(),
        };
        let res = self.client.txn_stream(req).await?.into_inner();
        Ok(res)
    }

    pub async fn reward_period(&mut self) -> Result<RewardPeriod> {
        let req = FollowerSubnetworkLastRewardHeightReqV1 {
            token_type: BlockchainTokenTypeV1::from(ProtoTokenType::Mobile).into(),
        };
        let res = self
            .client
            .subnetwork_last_reward_height(req)
            .await?
            .into_inner();

        Ok(RewardPeriod::new(res.reward_height + 1, res.height))
    }
}
