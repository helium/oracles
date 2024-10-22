use crate::{send_with_retry, GetSignature, Keypair, Pubkey, SolanaRpcError, SubDao};
use async_trait::async_trait;
use helium_crypto::PublicKeyBinary;
use helium_lib::{client, dc, token};
use serde::Deserialize;
use solana_sdk::{
    commitment_config::CommitmentConfig, signature::Signature, transaction::Transaction,
};
use std::convert::Infallible;
use std::{collections::HashMap, str::FromStr};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error>;

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, Self::Error>;

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error>;

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, Self::Error>;
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    rpc_url: String,
    burn_keypair: String,
    #[serde(default)]
    payers_to_monitor: Vec<String>,
}

impl Settings {
    pub fn payers_to_monitor(&self) -> Result<Vec<PublicKeyBinary>, SolanaRpcError> {
        self.payers_to_monitor
            .iter()
            .map(|payer| PublicKeyBinary::from_str(payer))
            .collect::<Result<_, _>>()
            .map_err(SolanaRpcError::from)
    }
}

pub struct SolanaRpc {
    sub_dao: SubDao,
    provider: client::SolanaRpcClient,
    keypair: Keypair,
    payers_to_monitor: Vec<PublicKeyBinary>,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings, sub_dao: SubDao) -> Result<Arc<Self>, SolanaRpcError> {
        let Ok(keypair) = Keypair::read_from_file(&settings.burn_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError);
        };

        let provider = client::SolanaRpcClient::new_with_commitment(
            settings.rpc_url.clone(),
            CommitmentConfig::finalized(),
        );

        // FIXME: The dc_burn_authority is fetched in helium-lib.
        // I'm not sure I understand what should happen to this check.
        //
        // let program_cache = BurnProgramCache::new(&provider, dc_mint, dnt_mint).await?;
        // if program_cache.dc_burn_authority != keypair.pubkey() {
        //     return Err(SolanaRpcError::InvalidKeypair);
        // }

        Ok(Arc::new(Self {
            sub_dao,
            provider,
            keypair,
            payers_to_monitor: settings.payers_to_monitor()?,
        }))
    }
}

impl AsRef<client::SolanaRpcClient> for SolanaRpc {
    fn as_ref(&self) -> &client::SolanaRpcClient {
        &self.provider
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Error = SolanaRpcError;
    type Transaction = Transaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        let payer_pubkey = Pubkey::try_from(payer.as_ref())?;
        let delegated_dc_key = SubDao::Iot.delegated_dc_key(&payer_pubkey.to_string());
        let escrow_account = SubDao::Iot.escrow_key(&delegated_dc_key);

        let amount = match token::balance_for_address(&self, &escrow_account).await? {
            Some(token_balance) => token_balance.amount.amount,
            None => {
                tracing::info!(%payer, "Account not found, no balance");
                0
            }
        };

        if self.payers_to_monitor.contains(payer) {
            metrics::gauge!(
                "balance",
                "payer" => payer.to_string()
            )
            .set(amount as f64);
        }

        Ok(amount)
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, Self::Error> {
        let payer = Pubkey::try_from(payer.as_ref())?;
        let tx = dc::burn_delegated(self, self.sub_dao, &self.keypair, amount, payer).await?;

        Ok(tx)
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), Self::Error> {
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };
        match send_with_retry!(self
            .provider
            .send_and_confirm_transaction_with_spinner_and_config(
                tx,
                CommitmentConfig::finalized(),
                config,
            )) {
            Ok(signature) => {
                tracing::info!(
                    transaction = %signature,
                    "Data credit burn successful",
                );
                Ok(())
            }
            Err(err) => {
                let signature = tx.get_signature();
                tracing::error!(
                    transaction = %signature,
                    "Data credit burn failed: {err:?}"
                );
                Err(SolanaRpcError::RpcClientError(Box::new(err)))
            }
        }
    }

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, Self::Error> {
        Ok(matches!(
            self.provider
                .get_signature_status_with_commitment_and_history(
                    txn,
                    CommitmentConfig::finalized(),
                    true,
                )
                .await?,
            Some(Ok(()))
        ))
    }
}

const FIXED_BALANCE: u64 = 1_000_000_000;

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

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.payer_balance(payer).await
        } else {
            Ok(FIXED_BALANCE)
        }
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, Self::Error> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_burn_transaction(payer, amount).await?,
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

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.confirm_transaction(txn).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}

pub struct MockTransaction {
    pub signature: Signature,
    pub payer: PublicKeyBinary,
    pub amount: u64,
}

impl GetSignature for MockTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signature
    }
}

#[async_trait]
impl SolanaNetwork for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;
    type Transaction = MockTransaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        Ok(*self.lock().await.get(payer).unwrap())
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<MockTransaction, Self::Error> {
        Ok(MockTransaction {
            signature: Signature::new_unique(),
            payer: payer.clone(),
            amount,
        })
    }

    async fn submit_transaction(&self, txn: &MockTransaction) -> Result<(), Self::Error> {
        *self.lock().await.get_mut(&txn.payer).unwrap() -= txn.amount;
        Ok(())
    }

    async fn confirm_transaction(&self, _txn: &Signature) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
