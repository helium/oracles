use crate::Result;
use helium_proto::{
    services::{Channel, Endpoint},
    FollowerTxnStreamReqV1, FollowerTxnStreamRespV1,
};
use http::Uri;
use std::time::Duration;
use tonic::Streaming;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

type FollowerClient = helium_proto::follower_client::FollowerClient<Channel>;

pub struct FollowerService {
    client: FollowerClient,
}

impl FollowerService {
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
        height: Option<u64>,
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
}
