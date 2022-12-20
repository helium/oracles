use crate::{settings::TxnClients, Error, Result, Settings};
use helium_proto::{
    services::{
        transaction::{self, TxnQueryReqV1, TxnQueryRespV1, TxnSubmitReqV1, TxnSubmitRespV1},
        Channel,
    },
    BlockchainTxn,
};
use rand::{seq::SliceRandom, thread_rng};

type TransactionClient = transaction::Client<Channel>;

#[derive(Debug, Clone)]
pub struct TransactionService {
    clients: TxnClients,
}

impl TransactionService {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            clients: settings.connect_transactions(),
        }
    }

    pub fn random_client(&self) -> Result<TransactionClient> {
        let mut rng = thread_rng();
        match self.clients.choose(&mut rng).cloned() {
            Some(client) => Ok(client),
            None => Err(Error::ClientNotFound),
        }
    }

    pub async fn submit(&mut self, txn: BlockchainTxn, key: &[u8]) -> Result<TxnSubmitRespV1> {
        let req = TxnSubmitReqV1 {
            txn: Some(txn),
            key: key.to_vec(),
        };

        let mut client = self.random_client()?;
        let res = client.submit(req).await?.into_inner();
        Ok(res)
    }

    pub async fn query(&mut self, key: &[u8]) -> Result<TxnQueryRespV1> {
        let req = TxnQueryReqV1 { key: key.to_vec() };
        let mut client = self.random_client()?;
        let res = client.query(req).await?.into_inner();
        Ok(res)
    }
}
