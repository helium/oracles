use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    mobile_session::{DataTransferEvent, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
use mobile_packet_verifier::{burner::Burner, pending_burns};
use solana::{
    burn::SolanaNetwork, sender, Signature, SolanaRpcError, SolanaTransaction, Transaction,
};
use sqlx::PgPool;
use tokio::sync::Mutex;

#[sqlx::test]
fn burn_checks_for_sufficient_balance(pool: PgPool) -> anyhow::Result<()> {
    let payer_insufficient = PublicKeyBinary::from(vec![1]);
    let payer_sufficient = PublicKeyBinary::from(vec![2]);
    const ORIGINAL_BALANCE: u64 = 10_000;

    // Initialize payers with balances
    let mut solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(payer_insufficient.clone(), ORIGINAL_BALANCE)
        .await;
    solana_network
        .insert(payer_sufficient.clone(), ORIGINAL_BALANCE)
        .await;

    // Add Data Transfer Sessiosn for both payers
    let mut txn = pool.begin().await?;
    let session_one = mk_data_transfer_session(payer_insufficient.clone(), 1_000_000_000); // exceeds balance
    let session_two = mk_data_transfer_session(payer_sufficient.clone(), 1_000_000); // within balance
    pending_burns::save(&mut txn, &session_one, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_two, Utc::now()).await?;
    txn.commit().await?;

    // Ensure we see 2 pending burns
    let pre_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(pre_burns.len(), 2, "2 burns for 2 payers");

    // Burn what we can
    let (valid_sessions_tx, mut rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(valid_sessions, solana_network.clone());
    burner.burn(&pool).await?;

    // 1 burn succeeded, the other payer has insufficient balance
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "1 burn left");

    // Ensure no pending transactions
    let pending = pending_burns::fetch_all_pending_txns(&pool).await?;
    assert!(pending.is_empty(), "pending txn should be removed");

    // Ensure balance for payers through solana mock
    assert_eq!(
        solana_network.payer_balance(&payer_insufficient).await,
        ORIGINAL_BALANCE,
        "original balance"
    );
    assert!(
        solana_network.payer_balance(&payer_sufficient).await < ORIGINAL_BALANCE,
        "reduced balance"
    );

    // Ensure successful data transfer sessions were output
    let mut written_sessions = vec![];
    while let Ok(session) = rx.try_recv() {
        written_sessions.push(session);
    }
    assert_eq!(written_sessions.len(), 1, "1 data transfer session written");

    Ok(())
}

#[sqlx::test]
async fn test_confirm_pending_txns(pool: PgPool) -> anyhow::Result<()> {
    let payer_one = PublicKeyBinary::from(vec![1]);

    let mut solana_network = TestSolanaClientMap::default();
    solana_network.insert(payer_one.clone(), 10_000).await;

    // First transaction is confirmed
    // Make submission time in past to bypass confirm txn sleep
    let confirmed_signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer_one,
        1_000,
        &confirmed_signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Second transaction is unconfirmed
    // Make submission time in past to bypass confirm txn sleep
    let unconfirmed_signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer_one,
        500,
        &unconfirmed_signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Tell Mock Solana which txn to confirm
    solana_network.add_confirmed(confirmed_signature).await;
    // solana_network.add_confirmed(unconfirmed_txn).await; // uncomment for failure

    assert_eq!(pending_burns::fetch_all_pending_txns(&pool).await?.len(), 2);
    pending_burns::confirm_pending_txns(&pool, &solana_network).await?;
    assert_eq!(pending_burns::fetch_all_pending_txns(&pool).await?.len(), 1);

    let remaining_txn = pending_burns::fetch_all_pending_txns(&pool).await?;
    assert_eq!(remaining_txn.len(), 1);
    assert_eq!(remaining_txn[0].amount, 500);

    Ok(())
}

fn mk_data_transfer_session(
    payer_key: PublicKeyBinary,
    rewardable_bytes: u64,
) -> DataTransferSessionReq {
    DataTransferSessionReq {
        data_transfer_usage: DataTransferEvent {
            pub_key: payer_key.clone().into(),
            upload_bytes: rewardable_bytes / 2,
            download_bytes: rewardable_bytes / 2,
            radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
            event_id: "event-id".to_string(),
            payer: payer_key.clone(),
            timestamp: Utc::now(),
            signature: vec![],
        },
        rewardable_bytes,
        pub_key: payer_key.into(),
        signature: vec![],
    }
}

#[derive(Debug, Clone)]
pub struct TestSolanaClientMap {
    payer_balances: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    txn_sig_to_payer: Arc<Mutex<HashMap<Signature, (PublicKeyBinary, u64)>>>,
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
            confirmed_txns: Default::default(),
        }
    }
}

impl TestSolanaClientMap {
    pub fn new(ledger: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>) -> Self {
        Self {
            payer_balances: ledger,
            txn_sig_to_payer: Default::default(),
            block_height: Instant::now(),
            confirmed_txns: Default::default(),
        }
    }
    pub async fn insert(&mut self, payer: PublicKeyBinary, amount: u64) {
        self.payer_balances.lock().await.insert(payer, amount);
    }

    async fn add_confirmed(&mut self, signature: Signature) {
        self.confirmed_txns.lock().await.insert(signature);
    }

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> u64 {
        self.payer_balances
            .lock()
            .await
            .get(payer)
            .cloned()
            .unwrap_or_default()
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
        // Test client must attempt to send for changes to take place
        sender::send_and_finalize(self, txn, store).await?;

        let signature = txn.get_signature();
        if let Some((payer, amount)) = self.txn_sig_to_payer.lock().await.get(signature) {
            *self.payer_balances.lock().await.get_mut(payer).unwrap() -= amount;
        }

        Ok(())
    }

    async fn confirm_transaction(&self, signature: &Signature) -> Result<bool, SolanaRpcError> {
        Ok(self.confirmed_txns.lock().await.contains(signature))
    }
}

#[async_trait::async_trait]
impl sender::SenderClientExt for TestSolanaClientMap {
    async fn send_txn(&self, txn: &Transaction) -> Result<Signature, sender::SolanaClientError> {
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
