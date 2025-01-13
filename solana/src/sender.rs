use std::time::Duration;

use exponential_backoff::Backoff;
use helium_lib::{client, keypair::Signature};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;

use crate::Transaction;

pub type SolanaClientError = solana_client::client_error::ClientError;
pub type SenderResult<T> = Result<T, SenderError>;

#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error("Txn Preparation error: {0}")]
    Preparation(String),
    #[error("Solana Client error: {0}")]
    SolanaClient(#[from] SolanaClientError),
}

impl SenderError {
    pub fn preparation(msg: &str) -> Self {
        Self::Preparation(msg.to_string())
    }
}

pub async fn send_and_finalize(
    client: &impl SenderClientExt,
    txn: &Transaction,
    store: &impl TxnStore,
) -> SenderResult<()> {
    let sent_block_height = client.get_block_height().await?;

    store.on_prepared(&txn).await?;
    send_with_retry(client, &txn, store).await?;
    store.on_sent(&txn).await;

    finalize_signature(client, &txn, store, sent_block_height).await?;
    store.on_finalized(&txn).await;

    Ok(())
}

async fn send_with_retry(
    client: &impl SenderClientExt,
    txn: &Transaction,
    store: &impl TxnStore,
) -> SenderResult<()> {
    let backoff = store.make_backoff().into_iter();

    for (attempt, duration) in backoff.enumerate() {
        match client.send_txn(txn).await {
            Ok(_sig) => return Ok(()),
            Err(err) => match duration {
                Some(duration) => {
                    store.on_sent_retry(txn, attempt + 1).await;
                    tokio::time::sleep(duration).await;
                }
                None => {
                    store.on_error_sending(txn, &err).await;
                    return Err(err.into());
                }
            },
        }
    }

    unreachable!("Exceeded max attempts without returning")
}

async fn finalize_signature(
    client: &impl SenderClientExt,
    txn: &Transaction,
    store: &impl TxnStore,
    sent_block_height: u64,
) -> SenderResult<()> {
    const FINALIZATION_BLOCK_COUNT: u64 = 152;

    // Sleep until we're past the block where our transaction should be finalized.
    loop {
        let curr_block_height = client.get_block_height().await?;
        if curr_block_height > sent_block_height + FINALIZATION_BLOCK_COUNT {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let signature = txn.get_signature();
    if let Err(err) = client.finalize_signature(signature).await {
        store.on_error_finalizing(txn, &err).await;
        return Err(err.into());
    };

    Ok(())
}

#[async_trait::async_trait]
pub trait SenderClientExt: Send + Sync {
    async fn send_txn(&self, txn: &Transaction) -> Result<Signature, SolanaClientError>;
    async fn finalize_signature(&self, signature: &Signature) -> Result<(), SolanaClientError>;
    async fn get_block_height(&self) -> Result<u64, SolanaClientError>;
}

#[async_trait::async_trait]
pub trait TxnStore: Send + Sync {
    fn make_backoff(&self) -> Backoff {
        Backoff::new(5, Duration::from_secs(1), Duration::from_secs(5))
    }
    // Last chance for _not_ send a transaction.
    async fn on_prepared(&self, _txn: &Transaction) -> SenderResult<()> {
        Ok(())
    }
    // The txn has been succesfully sent to Solana.
    async fn on_sent(&self, _txn: &Transaction) {
        tracing::info!("txn sent");
    }
    // Sending the txn failed, and we're going to try again.
    // If any sleeping should be done, do it here.
    async fn on_sent_retry(&self, _txn: &Transaction, attempt: usize) {
        tracing::info!(attempt, "txn retrying");
    }
    // Txn's status has been successfully seen as Finalized.
    // Everything is done.
    async fn on_finalized(&self, _txn: &Transaction) {}
    // Something went wrong sending, the txn never made it anywhere.
    async fn on_error_sending(&self, _txn: &Transaction, _err: &SolanaClientError) {}
    // Somethign went wrong finalizing, the txn was sent but not confirmed on chain.
    async fn on_error_finalizing(&self, _txn: &Transaction, _err: &SolanaClientError) {}
}

pub struct NoopStore;

#[async_trait::async_trait]
impl TxnStore for NoopStore {}

#[async_trait::async_trait]
impl<T: AsRef<client::SolanaRpcClient> + Send + Sync> SenderClientExt for T {
    async fn send_txn(&self, txn: &Transaction) -> Result<Signature, SolanaClientError> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };

        Ok(self
            .as_ref()
            .send_transaction_with_config(txn, config)
            .await?)
    }

    async fn finalize_signature(&self, signature: &Signature) -> Result<(), SolanaClientError> {
        // TODO: poll while checking against the block height.
        // Maybe return a different type of error here.
        Ok(self
            .as_ref()
            .poll_for_signature_with_commitment(signature, CommitmentConfig::finalized())
            .await?)
    }

    async fn get_block_height(&self) -> Result<u64, SolanaClientError> {
        Ok(self.as_ref().get_block_height().await?)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Instant,
    };

    use solana_sdk::signer::SignerError;

    use super::*;

    #[derive(Default)]
    struct MockTxnStore {
        pub fail_prepared: bool,
        pub calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockTxnStore {
        fn fail_prepared() -> Self {
            Self {
                fail_prepared: true,
                ..Default::default()
            }
        }
        fn record_call(&self, method: String) {
            self.calls.lock().unwrap().push(method);
        }
    }

    #[async_trait::async_trait]
    impl TxnStore for MockTxnStore {
        fn make_backoff(&self) -> Backoff {
            Backoff::new(5, Duration::from_millis(10), Duration::from_millis(50))
        }

        async fn on_prepared(&self, txn: &Transaction) -> SenderResult<()> {
            if self.fail_prepared {
                return Err(SenderError::preparation("mock failure"));
            }
            let signature = txn.get_signature();
            self.record_call(format!("on_prepared: {signature}"));
            Ok(())
        }
        async fn on_sent(&self, txn: &Transaction) {
            let signature = txn.get_signature();
            self.record_call(format!("on_sent: {signature}"));
        }
        async fn on_sent_retry(&self, txn: &Transaction, attempt: usize) {
            let signature = txn.get_signature();
            self.record_call(format!("on_sent_retry: {attempt} {signature}"));
        }
        async fn on_finalized(&self, txn: &Transaction) {
            let signature = txn.get_signature();
            self.record_call(format!("on_finalized: {signature}"))
        }
        async fn on_error_sending(&self, txn: &Transaction, _err: &SolanaClientError) {
            let signature = txn.get_signature();
            self.record_call(format!(
                "on_error_sending: {signature} could not submit 5 times"
            ));
        }
        async fn on_error_finalizing(&self, txn: &Transaction, _err: &SolanaClientError) {
            let signature = txn.get_signature();
            self.record_call(format!(
                "on_error_finalizing: {signature} could not finalize"
            ));
        }
    }

    struct MockClient {
        pub sent_attempts: Mutex<usize>,
        pub succeed_after_sent_attempts: usize,
        pub finalize_success: bool,
        pub block_height: Instant,
    }

    impl MockClient {
        fn succeed() -> Self {
            Self {
                sent_attempts: Mutex::new(0),
                succeed_after_sent_attempts: 0,
                finalize_success: true,
                block_height: Instant::now(),
            }
        }

        fn succeed_after(succeed_after_sent_attempts: usize) -> Self {
            Self {
                sent_attempts: Mutex::new(0),
                succeed_after_sent_attempts,
                finalize_success: true,
                block_height: Instant::now(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SenderClientExt for MockClient {
        async fn send_txn(&self, txn: &Transaction) -> Result<Signature, SolanaClientError> {
            let mut attempts = self.sent_attempts.lock().unwrap();
            *attempts += 1;

            if *attempts >= self.succeed_after_sent_attempts {
                return Ok(txn.get_signature().clone());
            }

            // Fake Error
            Err(SignerError::KeypairPubkeyMismatch.into())
        }

        async fn finalize_signature(
            &self,
            _signature: &Signature,
        ) -> Result<(), SolanaClientError> {
            if self.finalize_success {
                return Ok(());
            }
            // Fake Error
            Err(SignerError::KeypairPubkeyMismatch.into())
        }

        async fn get_block_height(&self) -> Result<u64, SolanaClientError> {
            // Using nanoseconds since test start as block_height
            let block_height = self.block_height.elapsed().as_nanos();
            Ok(block_height as u64)
        }
    }

    fn mk_test_transaction() -> Transaction {
        let mut inner = solana_sdk::transaction::Transaction::default();
        inner.signatures.push(Signature::new_unique());
        Transaction {
            inner,
            sent_block_height: 1,
        }
    }

    #[tokio::test]
    async fn send_finalized_success() -> anyhow::Result<()> {
        let tx = mk_test_transaction();
        let store = MockTxnStore::default();
        let client = MockClient::succeed();

        let _ = send_and_finalize(&client, &tx, &store).await?;

        let signature = tx.get_signature();
        let calls = store.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![
                format!("on_prepared: {signature}"),
                format!("on_sent: {signature}"),
                format!("on_finalized: {signature}")
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_finalized_success_after_retry() -> anyhow::Result<()> {
        let txn = mk_test_transaction();
        let store = MockTxnStore::default();
        let client = MockClient::succeed_after(5);

        let _ = send_and_finalize(&client, &txn, &store).await?;

        let signature = txn.get_signature();
        let calls = store.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![
                format!("on_prepared: {signature}"),
                format!("on_sent_retry: 1 {signature}"),
                format!("on_sent_retry: 2 {signature}"),
                format!("on_sent_retry: 3 {signature}"),
                format!("on_sent_retry: 4 {signature}"),
                format!("on_sent: {signature}"),
                format!("on_finalized: {signature}")
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_error_with_retry() -> anyhow::Result<()> {
        let txn = mk_test_transaction();
        let store = MockTxnStore::default();
        let client = MockClient::succeed_after(999);

        let res = send_and_finalize(&client, &txn, &store).await;
        assert!(res.is_err());

        let signature = txn.get_signature();
        let calls = store.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![
                format!("on_prepared: {signature}"),
                format!("on_sent_retry: 1 {signature}"),
                format!("on_sent_retry: 2 {signature}"),
                format!("on_sent_retry: 3 {signature}"),
                format!("on_sent_retry: 4 {signature}"),
                format!("on_error_sending: {signature} could not submit 5 times")
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn send_success_finalize_error() -> anyhow::Result<()> {
        let txn = mk_test_transaction();
        let store = MockTxnStore::default();
        let mut client = MockClient::succeed();
        client.finalize_success = false;

        let res = send_and_finalize(&client, &txn, &store).await;
        assert!(res.is_err());

        let signature = txn.get_signature();
        let calls = store.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![
                format!("on_prepared: {signature}"),
                format!("on_sent: {signature}"),
                format!("on_error_finalizing: {signature} could not finalize")
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn failed_preparation() -> anyhow::Result<()> {
        let txn = mk_test_transaction();
        let store = MockTxnStore::fail_prepared();
        let client = MockClient::succeed();

        let res = send_and_finalize(&client, &txn, &store).await;
        assert!(res.is_err());

        let calls = store.calls.lock().unwrap();
        assert_eq!(*calls, Vec::<String>::new());

        Ok(())
    }
}
