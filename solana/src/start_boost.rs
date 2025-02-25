use crate::{read_keypair_from_file, GetSignature, SolanaRpcError, Transaction};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use file_store::hex_boost::BoostedHexActivation;
use helium_lib::{
    boosting, client,
    keypair::{Keypair, Signature},
    TransactionOpts,
};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signer::Signer};
use std::sync::Arc;

macro_rules! send_with_retry {
    ($rpc:expr) => {{
        let mut attempt = 1;
        loop {
            match $rpc.await {
                Ok(resp) => break Ok(resp),
                Err(err) => {
                    if attempt < 5 {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(attempt)).await;
                        continue;
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    }};
}

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, SolanaRpcError>;

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<(), SolanaRpcError>;

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, SolanaRpcError>;
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
    transaction_opts: TransactionOpts,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>, SolanaRpcError> {
        let Ok(keypair) = read_keypair_from_file(&settings.start_authority_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError(
                settings.start_authority_keypair.to_owned(),
            ));
        };
        let provider = client::SolanaRpcClient::new_with_commitment(
            settings.rpc_url.clone(),
            CommitmentConfig::finalized(),
        );

        Ok(Arc::new(Self {
            provider,
            start_authority: keypair.pubkey(),
            keypair,
            transaction_opts: TransactionOpts::default(),
        }))
    }
}

impl AsRef<client::SolanaRpcClient> for SolanaRpc {
    fn as_ref(&self) -> &client::SolanaRpcClient {
        &self.provider
    }
}

struct BoostedHex<'a>(&'a Pubkey, &'a BoostedHexActivation);

impl helium_lib::boosting::StartBoostingHex for BoostedHex<'_> {
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
    type Transaction = Transaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, SolanaRpcError> {
        let tx = boosting::start_boost(
            &self,
            batch.iter().map(|b| BoostedHex(&self.start_authority, b)),
            &self.keypair,
            &self.transaction_opts,
        )
        .await?;

        Ok(tx.into())
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), SolanaRpcError> {
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
                Err(err.into())
            }
        }
    }

    async fn confirm_transaction(&self, signature: &str) -> Result<bool, SolanaRpcError> {
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
    type Transaction = PossibleTransaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, SolanaRpcError> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_start_boost_transaction(batch).await?,
            ))
        } else {
            Ok(PossibleTransaction::NoTransaction(Signature::new_unique()))
        }
    }

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<(), SolanaRpcError> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn).await?
            }
            (None, PossibleTransaction::NoTransaction(_)) => (),
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, SolanaRpcError> {
        if let Some(ref rpc) = self {
            rpc.confirm_transaction(txn).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}
