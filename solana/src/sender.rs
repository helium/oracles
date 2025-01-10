use std::time::Duration;

use exponential_backoff::Backoff;
use helium_lib::{client, Signature, TransactionWithBlockhash};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;

pub use helium_lib::send_txn::SolanaClientError;

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
    txn: &TransactionWithBlockhash,
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
    txn: &TransactionWithBlockhash,
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
    txn: &TransactionWithBlockhash,
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
    async fn send_txn(
        &self,
        txn: &TransactionWithBlockhash,
    ) -> Result<Signature, SolanaClientError>;
    async fn finalize_signature(&self, signature: &Signature) -> Result<(), SolanaClientError>;
    async fn get_block_height(&self) -> Result<u64, SolanaClientError>;
}

#[async_trait::async_trait]
pub trait TxnStore: Send + Sync {
    fn make_backoff(&self) -> Backoff {
        Backoff::new(5, Duration::from_millis(50), Duration::from_secs(5))
    }
    // Last chance for _not_ send a transaction.
    async fn on_prepared(&self, _txn: &TransactionWithBlockhash) -> SenderResult<()> {
        Ok(())
    }
    // The txn has been succesfully sent to Solana.
    async fn on_sent(&self, _txn: &TransactionWithBlockhash) {}
    // Sending the txn failed, and we're going to try again.
    // If any sleeping should be done, do it here.
    async fn on_sent_retry(&self, _txn: &TransactionWithBlockhash, _attempt: usize) {}
    // Txn's status has been successfully seen as Finalized.
    // Everything is done.
    async fn on_finalized(&self, _txn: &TransactionWithBlockhash) {}
    // Something went wrong sending, the txn never made it anywhere.
    async fn on_error_sending(&self, _txn: &TransactionWithBlockhash, _err: &SolanaClientError) {}
    // Somethign went wrong finalizing, the txn was sent but not confirmed on chain.
    async fn on_error_finalizing(&self, _txn: &TransactionWithBlockhash, _err: &SolanaClientError) {
    }
}

pub struct NoopStore;

#[async_trait::async_trait]
impl TxnStore for NoopStore {}

#[async_trait::async_trait]
impl<T: AsRef<client::SolanaRpcClient> + Send + Sync> SenderClientExt for T {
    async fn send_txn(
        &self,
        txn: &TransactionWithBlockhash,
    ) -> Result<Signature, SolanaClientError> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };

        Ok(self
            .as_ref()
            .send_transaction_with_config(txn.inner_txn(), config)
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
