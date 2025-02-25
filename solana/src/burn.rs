use crate::{
    read_keypair_from_file, sender, GetSignature, Keypair, SolanaRpcError, SubDao, Transaction,
};
use async_trait::async_trait;
use helium_crypto::PublicKeyBinary;
use helium_lib::{client, dc, token, TransactionOpts};
use serde::Deserialize;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use std::str::FromStr;
use std::sync::Arc;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Transaction: Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError>;

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, SolanaRpcError>;

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
        store: &impl sender::TxnStore,
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
        let keypair = read_keypair_from_file(&settings.burn_keypair)?;
        let provider = client::SolanaRpcClient::new_with_commitment(
            settings.rpc_url.clone(),
            CommitmentConfig::finalized(),
        );

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
    type Transaction = Transaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
        let delegated_dc_key = self.sub_dao.delegated_dc_key(payer);
        let escrow_account = self.sub_dao.escrow_key(&delegated_dc_key);

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
        let tx = dc::burn_delegated(
            self,
            self.sub_dao,
            &self.keypair,
            amount,
            payer,
            &self.transaction_opts,
        )
        .await?;

        Ok(tx.into())
    }

    async fn submit_transaction(
        &self,
        tx: &Self::Transaction,
        store: &impl sender::TxnStore,
    ) -> Result<(), SolanaRpcError> {
        match sender::send_and_finalize(&self, tx, store).await {
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
        store: &impl sender::TxnStore,
    ) -> Result<(), SolanaRpcError> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn, store).await?
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

pub mod test_client {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
        time::Instant,
    };

    use helium_crypto::PublicKeyBinary;
    use helium_lib::keypair::Signature;
    use tokio::sync::Mutex;

    use crate::{sender, SolanaRpcError, SolanaTransaction, Transaction};

    use super::SolanaNetwork;

    #[derive(Debug, Clone)]
    pub struct TestSolanaClientMap {
        payer_balances: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
        txn_sig_to_payer: Arc<Mutex<HashMap<Signature, (PublicKeyBinary, u64)>>>,
        confirm_all_txns: bool,
        fail_on_submit_txn: bool,
        confirmed_txns: Arc<Mutex<HashSet<Signature>>>,
        // Using the nanoseconds since the client was made as block height
        block_height: Instant,
    }

    impl Default for TestSolanaClientMap {
        fn default() -> Self {
            Self {
                payer_balances: Default::default(),
                txn_sig_to_payer: Default::default(),
                block_height: Instant::now(),
                confirm_all_txns: true,
                fail_on_submit_txn: false,
                confirmed_txns: Default::default(),
            }
        }
    }

    impl TestSolanaClientMap {
        pub fn fail_on_send() -> Self {
            Self {
                fail_on_submit_txn: true,
                ..Default::default()
            }
        }
        pub fn new(ledger: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>) -> Self {
            Self {
                payer_balances: ledger,
                txn_sig_to_payer: Default::default(),
                block_height: Instant::now(),
                confirm_all_txns: true,
                fail_on_submit_txn: false,
                confirmed_txns: Default::default(),
            }
        }
        pub async fn insert(&mut self, payer: PublicKeyBinary, amount: u64) {
            self.payer_balances.lock().await.insert(payer, amount);
        }

        pub async fn add_confirmed(&mut self, signature: Signature) {
            self.confirm_all_txns = false;
            self.confirmed_txns.lock().await.insert(signature);
        }

        pub async fn get_payer_balance(&self, payer: &PublicKeyBinary) -> u64 {
            self.payer_balances
                .lock()
                .await
                .get(payer)
                .cloned()
                .unwrap_or_default()
        }

        pub async fn set_payer_balance(&self, payer: &PublicKeyBinary, balance: u64) {
            *self.payer_balances.lock().await.get_mut(payer).unwrap() = balance;
        }
    }

    #[async_trait::async_trait]
    impl SolanaNetwork for TestSolanaClientMap {
        type Transaction = Transaction;

        async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
            Ok(*self.payer_balances.lock().await.get(payer).unwrap())
        }

        async fn make_burn_transaction(
            &self,
            payer: &PublicKeyBinary,
            amount: u64,
        ) -> Result<Transaction, SolanaRpcError> {
            let mut inner = SolanaTransaction::default();

            let sig = Signature::new_unique();
            // add signature -> (payer, amount) so we can subtract
            self.txn_sig_to_payer
                .lock()
                .await
                .insert(sig, (payer.clone(), amount));
            inner.signatures.push(sig);

            Ok(Transaction {
                inner,
                sent_block_height: 1,
            })
        }

        async fn submit_transaction(
            &self,
            txn: &Transaction,
            store: &impl sender::TxnStore,
        ) -> Result<(), SolanaRpcError> {
            if self.fail_on_submit_txn {
                panic!("attempting to send transaction");
            }
            // Test client must attempt to send for changes to take place
            sender::send_and_finalize(self, txn, store).await?;

            let signature = txn.get_signature();
            if let Some((payer, amount)) = self.txn_sig_to_payer.lock().await.get(signature) {
                *self.payer_balances.lock().await.get_mut(payer).unwrap() -= amount;
            }

            Ok(())
        }

        async fn confirm_transaction(&self, signature: &Signature) -> Result<bool, SolanaRpcError> {
            if self.confirm_all_txns {
                return Ok(true);
            }
            Ok(self.confirmed_txns.lock().await.contains(signature))
        }
    }

    #[async_trait::async_trait]
    impl sender::SenderClientExt for TestSolanaClientMap {
        async fn send_txn(
            &self,
            txn: &Transaction,
        ) -> Result<Signature, sender::SolanaClientError> {
            Ok(*txn.get_signature())
        }
        async fn finalize_signature(
            &self,
            _signature: &Signature,
        ) -> Result<(), sender::SolanaClientError> {
            Ok(())
        }
        async fn get_block_height(&self) -> Result<u64, sender::SolanaClientError> {
            // Using the nanoseconds since the client was made as block height
            let block_height = self.block_height.elapsed().as_nanos();
            Ok(block_height as u64)
        }
    }
}
