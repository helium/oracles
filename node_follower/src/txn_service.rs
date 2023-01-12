use crate::{Error, Result, Settings};
use helium_proto::{
    services::{
        transaction::{self, TxnQueryReqV1, TxnQueryRespV1, TxnSubmitReqV1, TxnSubmitRespV1},
        Channel,
    },
    BlockchainTxn,
};
use rand::{seq::SliceRandom, thread_rng};

pub type TransactionClient = transaction::Client<Channel>;
pub type TransactionClients = Vec<TransactionClient>;

#[derive(Debug, Clone)]
pub struct TransactionService {
    // Required to create TransactionService, built using the url from settings
    client: TransactionClient,
    // Optional, create multiple clients using submission_urls from settings.
    // For now, only poc-injector configures this in its settings.
    clients: Option<TransactionClients>,
}

impl TransactionService {
    pub fn from_settings(settings: &Settings) -> Self {
        let clients = match &settings.submission_urls {
            None => None,
            Some(_urls) => settings.connect_multiple_transactions(),
        };
        Self {
            client: settings.connect_transactions(),
            clients,
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

    pub async fn submit_random(
        &mut self,
        txn: BlockchainTxn,
        key: &[u8],
    ) -> Result<TxnSubmitRespV1> {
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
        let res = self.client.query(req).await?.into_inner();
        Ok(res)
    }

    pub fn random_client(&self) -> Result<TransactionClient> {
        let mut rng = thread_rng();
        match &self.clients {
            Some(clients) => match clients.choose(&mut rng).cloned() {
                Some(client) => Ok(client),
                None => Err(Error::ClientNotFound),
            },
            None => {
                // Use the singular transaction client if there are no configured optional multiple
                // clients
                Ok(self.client.clone())
            }
        }
    }
}
