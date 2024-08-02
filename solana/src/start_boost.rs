use crate::{send_with_retry, GetSignature, Keypair, Pubkey, SolanaRpcError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use file_store::hex_boost::BoostedHexActivation;
use helium_lib::{boosting, client};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::{sync::Arc, time::Duration};

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error>;

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error>;

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, Self::Error>;
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    rpc_url: String,
    start_authority_keypair: String,
}
pub struct SolanaRpc {
    provider: RpcClient,
    keypair: Keypair,
    start_authority: Pubkey,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>, SolanaRpcError> {
        let Ok(keypair) = read_keypair_file(&settings.start_authority_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError);
        };
        let provider = client::SolanaRpcClient::new_with_commitment(
            settings.rpc_url.clone(),
            CommitmentConfig::finalized(),
        );

        Ok(Arc::new(Self {
            provider,
            start_authority: keypair.pubkey(),
            keypair: keypair.into(),
        }))
    }
}

impl AsRef<client::SolanaRpcClient> for SolanaRpc {
    fn as_ref(&self) -> &client::SolanaRpcClient {
        &self.provider
    }
}

struct BoostedHex<'a>(&'a Pubkey, &'a BoostedHexActivation);

impl<'a> helium_lib::boosting::StartBoostingHex for BoostedHex<'a> {
    fn start_authority(&self) -> Pubkey {
        *self.0
    }

    fn boost_config(&self) -> Pubkey {
        self.1.boost_config_pubkey.parse().unwrap()
    }

    fn boosted_hex(&self) -> Pubkey {
        self.1.boosted_hex_pubkey.parse().unwrap()
    }

    fn activation_ts(&self) -> DateTime<Utc> {
        self.1.activation_ts
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Error = SolanaRpcError;
    type Transaction = Transaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error> {
        let tx = boosting::start_boost(
            &self,
            &self.keypair,
            batch.iter().map(|b| BoostedHex(&self.start_authority, b)),
        )
        .await?;

        Ok(tx)
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), Self::Error> {
        match send_with_retry!(self.provider.send_and_confirm_transaction(tx)) {
            Ok(signature) => {
                tracing::info!(
                    transaction = %signature,
                    "hex start boost successful",
                );
                Ok(())
            }
            Err(err) => {
                let signature = tx.get_signature();
                tracing::error!(
                    transaction = %signature,
                    "hex start boost failed: {err:?}"
                );
                Err(SolanaRpcError::RpcClientError(Box::new(err)))
            }
        }
    }

    async fn confirm_transaction(&self, signature: &str) -> Result<bool, Self::Error> {
        let txn: Signature = signature.parse()?;
        Ok(matches!(
            self.provider
                .get_signature_status_with_commitment_and_history(
                    &txn,
                    CommitmentConfig::finalized(),
                    true,
                )
                .await?,
            Some(Ok(()))
        ))
    }
}
pub enum PossibleTransaction {
    NoTransaction(Signature),
    Transaction(Transaction),
}

impl GetSignature for PossibleTransaction {
    fn get_signature(&self) -> &Signature {
        match self {
            Self::NoTransaction(ref sig) => sig,
            Self::Transaction(ref txn) => txn.get_signature(),
        }
    }
}

#[async_trait]
impl SolanaNetwork for Option<Arc<SolanaRpc>> {
    type Error = SolanaRpcError;
    type Transaction = PossibleTransaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_start_boost_transaction(batch).await?,
            ))
        } else {
            Ok(PossibleTransaction::NoTransaction(Signature::new_unique()))
        }
    }

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn).await?
            }
            (None, PossibleTransaction::NoTransaction(_)) => (),
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.confirm_transaction(txn).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}
