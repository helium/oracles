use crate::Result;
use helium_proto::{
    services::{
        transaction::{self, TxnQueryReqV1, TxnQueryRespV1, TxnSubmitReqV1, TxnSubmitRespV1},
        Channel, Endpoint,
    },
    BlockchainTxn,
};
use http::Uri;
use std::{env, str::FromStr, time::Duration};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

type TransactionClient = transaction::Client<Channel>;

#[derive(Debug, Clone)]
pub struct TransactionService {
    client: TransactionClient,
}

impl TransactionService {
    pub fn from_env() -> Result<Self> {
        let uri = Uri::from_str(
            &env::var("TRANSACTION_URI").unwrap_or_else(|_| DEFAULT_URI.to_string()),
        )?;
        Self::new(uri)
    }

    pub fn new(uri: Uri) -> Result<Self> {
        let channel = Endpoint::from(uri)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: TransactionClient::new(channel),
        })
    }

    pub async fn submit(&mut self, txn: BlockchainTxn, key: &[u8]) -> Result<TxnSubmitRespV1> {
        let req = TxnSubmitReqV1 {
            txn: Some(txn),
            key: key.to_vec(),
        };
        let res = self.client.submit(req).await?.into_inner();
        Ok(res)
    }

    pub async fn query(&mut self, key: &[u8]) -> Result<TxnQueryRespV1> {
        let req = TxnQueryReqV1 { key: key.to_vec() };
        let res = self.client.query(req).await?.into_inner();
        Ok(res)
    }
}
