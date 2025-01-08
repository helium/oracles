use crate::{
    balances::{BalanceCache, BalanceStore},
    pending::{
        confirm_pending_txns, Burn, ConfirmPendingError, PendingTables, PendingTablesTransaction,
    },
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKeyBinary;
use solana::{burn::SolanaNetwork, SolanaRpcError};
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

pub struct Burner<P, S> {
    pending_tables: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

impl<P, S> ManagedTask for Burner<P, S>
where
    P: PendingTables,
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Solana error: {0}")]
    SolanaError(#[from] SolanaRpcError),
    #[error("Confirm pending transaction error: {0}")]
    ConfirmPendingError(#[from] ConfirmPendingError),
}

impl<P, S> Burner<P, S> {
    pub fn new(
        pending_tables: P,
        balances: &BalanceCache<S>,
        burn_period: Duration,
        solana: S,
    ) -> Self {
        Self {
            pending_tables,
            balances: balances.balances(),
            burn_period,
            solana,
        }
    }
}

impl<P, S> Burner<P, S>
where
    P: PendingTables + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<(), BurnError> {
        tracing::info!("Starting burner");
        let mut burn_timer = time::interval(self.burn_period);
        burn_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = burn_timer.tick() => {
                    match self.burn().await {
                        Ok(()) => continue,
                        Err(err) => {
                            tracing::error!("Error while burning data credits: {err}");
                            confirm_pending_txns(&self.pending_tables, &self.solana, &self.balances).await?;
                        }
                    }
                }
            }
        }
        tracing::info!("Stopping burner");
        Ok(())
    }

    pub async fn burn(&mut self) -> Result<(), BurnError> {
        // Fetch the next payer and amount that should be burn. If no such burn
        // exists, perform no action.
        let Some(Burn { payer, amount }) = self.pending_tables.fetch_next_burn().await? else {
            return Ok(());
        };

        tracing::info!(%amount, %payer, "Burning DC");

        let txn = self
            .solana
            .make_burn_transaction(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        let store = BurnTxnStore::new(
            self.pending_tables.clone(),
            self.balances.clone(),
            payer,
            amount,
        );

        self.solana
            .submit_transaction(&txn, &store, 5, Duration::from_millis(500))
            .map_err(BurnError::SolanaError)
            .await
    }
}

pub struct BurnTxnStore<PT> {
    pool: PT,
    balances: BalanceStore,
    payer: PublicKeyBinary,
    amount: u64,
}

impl<PT: PendingTables + Clone> BurnTxnStore<PT> {
    pub fn new(pool: PT, balances: BalanceStore, payer: PublicKeyBinary, amount: u64) -> Self {
        Self {
            pool,
            balances,
            payer,
            amount,
        }
    }
}

#[async_trait::async_trait]
impl<PT: PendingTables> solana::send_txn::TxnStore for BurnTxnStore<PT> {
    fn make_span(&self) -> tracing::Span {
        tracing::info_span!(
            "burn_txn",
            payer = %self.payer,
            amount = self.amount
        )
    }

    async fn on_prepared(
        &self,
        signature: &solana::Signature,
    ) -> Result<(), solana::send_txn::TxnStoreError> {
        tracing::info!("txn prepared");

        let add_pending = self
            .pool
            .add_pending_transaction(&self.payer, self.amount, signature);

        let Ok(()) = add_pending.await else {
            tracing::error!("failed to add pending transcation");
            return Err(solana::send_txn::TxnStoreError::new(
                "could not add pending transaction",
            ));
        };

        Ok(())
    }

    async fn on_sent(&self, _signature: &solana::Signature) {
        tracing::info!("txn submitted");
    }

    async fn on_sent_retry(&self, _signature: &solana::Signature, attempt: usize) {
        tracing::warn!(attempt, "retrying");
    }

    async fn on_finalized(&self, signature: &solana::Signature) {
        tracing::info!("txn finalized");
        let Ok(mut txn) = self.pool.begin().await else {
            tracing::error!("failed to start finalized txn transaction");
            return;
        };

        let Ok(()) = txn.remove_pending_transaction(signature).await else {
            tracing::error!("failed to remove pending");
            return;
        };

        let Ok(()) = txn.subtract_burned_amount(&self.payer, self.amount).await else {
            tracing::error!("failed to subtract burned amount");
            return;
        };

        // Subtract balances from map before submitted db txn
        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(&self.payer).unwrap();
        // Reduce the pending burn amount and the payer's balance by the amount we've burned
        payer_account.burned = payer_account.burned.saturating_sub(self.amount);
        payer_account.balance = payer_account.balance.saturating_sub(self.amount);

        let Ok(()) = txn.commit().await else {
            tracing::error!("failed to commit finalized transaction");
            return;
        };

        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "true"
        )
        .increment(self.amount);
    }

    async fn on_error(&self, signature: &solana::Signature, err: solana::send_txn::TxnSenderError) {
        tracing::warn!(?err, "txn failed");

        let Ok(mut txn) = self.pool.begin().await else {
            tracing::error!("failed to start error transaction");
            return;
        };

        let Ok(()) = txn.remove_pending_transaction(signature).await else {
            tracing::error!("failed to remove pending transaction on error");
            return;
        };

        let Ok(()) = txn.commit().await else {
            tracing::error!("failed to commit on error transaction");
            return;
        };

        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "false"
        )
        .increment(self.amount);
    }
}
