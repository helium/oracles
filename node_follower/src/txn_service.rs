use crate::{Result, Settings};
use helium_proto::{
    services::{
        transaction::{self, TxnQueryReqV1, TxnQueryRespV1, TxnSubmitReqV1, TxnSubmitRespV1},
        Channel,
    },
    BlockchainTxn,
};

type TransactionClient = transaction::Client<Channel>;

#[derive(Debug, Clone)]
pub struct TransactionService {
    client: TransactionClient,
}

impl TransactionService {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            client: settings.connect_transactions(),
        }
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
