use crate::{read_keypair_from_file, GetSignature, Keypair, Pubkey, SolanaRpcError, SubDao};
use async_trait::async_trait;
use helium_crypto::PublicKeyBinary;
use helium_lib::send_txn::{SolanaClientError, TxnSender, TxnSenderClientExt};
use helium_lib::{client, dc, send_txn, token, TransactionOpts, TransactionWithBlockhash};
use serde::Deserialize;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::Mutex;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError>;

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, SolanaRpcError>;

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
        store: &impl send_txn::TxnStore,
        max_attempts: usize,
        retry_delay: Duration,
    ) -> Result<(), SolanaRpcError>;

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError>;
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
    transaction_opts: TransactionOpts,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings, sub_dao: SubDao) -> Result<Arc<Self>, SolanaRpcError> {
        let Ok(keypair) = read_keypair_from_file(&settings.burn_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError(
                settings.burn_keypair.to_owned(),
            ));
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
            transaction_opts: TransactionOpts::default(),
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
    type Transaction = TransactionWithBlockhash;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
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
    ) -> Result<Self::Transaction, SolanaRpcError> {
        let payer = Pubkey::try_from(payer.as_ref())?;
        let tx = dc::burn_delegated(
            self,
            self.sub_dao,
            &self.keypair,
            amount,
            payer,
            &self.transaction_opts,
        )
        .await?;

        Ok(tx)
    }

    async fn submit_transaction(
        &self,
        tx: &Self::Transaction,
        store: &impl send_txn::TxnStore,
        max_attempts: usize,
        retry_delay: Duration,
    ) -> Result<(), SolanaRpcError> {
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };

        let sender = TxnSender::new(self, tx)
            .finalized(true)
            .with_store(store)
            .with_retry(max_attempts, retry_delay)
            .send(config)
            .await;

        match sender {
            Ok(_tracked) => {
                let signature = tx.get_signature();
                tracing::info!(
                    transaction = %signature,
                    "Data credit burn successful"
                );
                Ok(())
            }
            Err(err) => {
                let signature = tx.get_signature();
                tracing::error!(
                    transaction = %signature,
                    "Data credit burn failed: {err:?}"
                );
                Err(err.into())
            }
        }
    }

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError> {
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
    Transaction(TransactionWithBlockhash),
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

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
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
    ) -> Result<Self::Transaction, SolanaRpcError> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_burn_transaction(payer, amount).await?,
            ))
        } else {
            Ok(PossibleTransaction::NoTransaction(Signature::new_unique()))
        }
    }

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
        store: &impl send_txn::TxnStore,
        max_attempts: usize,
        retry_delay: Duration,
    ) -> Result<(), SolanaRpcError> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn, store, max_attempts, retry_delay)
                    .await?
            }
            (None, PossibleTransaction::NoTransaction(_)) => (),
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError> {
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

#[derive(Default, Clone)]
pub struct TestSolanaClientMap {
    pub payer_balances: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    pub txn_sig_to_payer: Arc<Mutex<HashMap<Signature, (PublicKeyBinary, u64)>>>,
}

impl TestSolanaClientMap {
    pub fn new(ledger: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>) -> Self {
        Self {
            payer_balances: ledger,
            txn_sig_to_payer: Default::default(),
        }
    }
    pub async fn insert(&mut self, payer: PublicKeyBinary, amount: u64) {
        self.payer_balances.lock().await.insert(payer, amount);
    }
}

#[async_trait]
impl SolanaNetwork for TestSolanaClientMap {
    type Transaction = TransactionWithBlockhash;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
        Ok(*self.payer_balances.lock().await.get(payer).unwrap())
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<TransactionWithBlockhash, SolanaRpcError> {
        let mut inner = solana_sdk::transaction::Transaction::default();

        let sig = Signature::new_unique();
        // add signature -> (payer, amount) so we can subtract
        self.txn_sig_to_payer
            .lock()
            .await
            .insert(sig, (payer.clone(), amount));
        inner.signatures.push(sig);

        Ok(TransactionWithBlockhash {
            inner,
            block_height: 1,
        })
    }

    async fn submit_transaction(
        &self,
        txn: &TransactionWithBlockhash,
        store: &impl send_txn::TxnStore,
        _max_attempts: usize,
        _retry_delay: Duration,
    ) -> Result<(), SolanaRpcError> {
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };

        let _sender = TxnSender::new(self, txn)
            .finalized(true)
            .with_store(store)
            .send(config)
            .await?;

        if let Some((payer, amount)) = self.txn_sig_to_payer.lock().await.get(txn.get_signature()) {
            *self.payer_balances.lock().await.get_mut(payer).unwrap() -= amount;
        }

        //*self.0.lock().await.get_mut(&txn.payer).unwrap() -= txn.amount;
        Ok(())
    }

    async fn confirm_transaction(&self, _txn: &Signature) -> Result<bool, SolanaRpcError> {
        Ok(true)
    }
}

#[async_trait::async_trait]
impl TxnSenderClientExt for TestSolanaClientMap {
    async fn send_txn(
        &self,
        txn: &TransactionWithBlockhash,
        _config: solana_client::rpc_config::RpcSendTransactionConfig,
    ) -> Result<Signature, SolanaClientError> {
        Ok(*txn.get_signature())
    }
    async fn finalize_signature(&self, _signature: &Signature) -> Result<(), SolanaClientError> {
        Ok(())
    }
    async fn get_block_height(&self) -> Result<u64, SolanaClientError> {
        Ok(1)
    }
}
